package filestore

import (
	"bufio"
	"crypto/md5"
	"encoding/base32"
	"encoding/hex"
	"hash/crc32"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// tmpfileName используется в качестве имени временного файла при генерации ошибок
const tmpfileName = "<temporary file>"

// Store описывает хранилище файлов.
type Store struct {
	dir string

	mutexes struct {
		sync.Mutex
		sync.Once
		m map[string]*sync.Mutex
	}
}

// FileInfo описывает информацию о сохраненном файле.
type FileInfo struct {
	Location string `json:"location"`
	Name     string `json:"name"`
	Mimetype string `json:"mimetype"`
	Size     int64  `json:"size"`
	CRC32    uint32 `json:"crc32"`
	MD5      string `json:"md5"`
}

// NewStore открывает и возвращает хранилище файлов.
func NewStore(dir string) (*Store, error) {
	// Создаём каталог, если он ещё не создан
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	return &Store{dir: dir}, nil
}

// Create сохраняет файл в хранилище. В качестве имени файла используется комбинация из двух хешей.
func (s *Store) Create(r io.Reader) (*FileInfo, error) {
	// Создаём временный файл в корневом каталоге
	tmpfile, err := os.CreateTemp(s.dir, "~tmp")
	if err != nil {
		err.(*os.PathError).Path = tmpfileName // Подмениваем имя файла
		return nil, err
	}
	defer os.Remove(tmpfile.Name())

	// Копируем содержимое во временный файл
	bufferReader := bufio.NewReaderSize(r, 4<<10)

	// Пытаемся определить тип содержимого
	data, err := bufferReader.Peek(512) // Читаем первые 512 байт файла
	if err != nil && err != io.EOF {
		tmpfile.Close()
		err = &os.PathError{Op: "create", Path: tmpfileName, Err: err}
		return nil, err
	}
	mimetype := http.DetectContentType(data)

	// Одновременно с сохранением в файл считаем две хеш-суммы
	hashCRC32, hashMD5 := crc32.NewIEEE(), md5.New()
	size, err := bufferReader.WriteTo(io.MultiWriter(tmpfile, hashCRC32, hashMD5))
	if err != nil {
		tmpfile.Close()
		err = &os.PathError{Op: "write", Path: tmpfileName, Err: err}
		return nil, err
	}

	// Формируем информацию о файле
	sumMD5 := hashMD5.Sum(nil)
	name := base32.StdEncoding.EncodeToString(append(hashCRC32.Sum(nil), sumMD5...))
	fi := &FileInfo{
		Location: s.GetFullName(name),
		Name:     name,
		Mimetype: mimetype,
		Size:     size,
		CRC32:    hashCRC32.Sum32(),
		MD5:      hex.EncodeToString(sumMD5),
	}

	// Закрываем временный файл
	if err := tmpfile.Close(); err != nil {
		if _, ok := err.(*os.PathError); ok {
			err.(*os.PathError).Path = tmpfileName
		}
		return nil, err
	}

	// Если файл уже существует, то просто обновляем его время создания
	now := time.Now()
	if err := os.Chtimes(fi.Location, now, now); err == nil {
		return fi, nil // Возвращаем информацию о файле, временный файл будет автоматически удален
	}

	// Если такого файла нет, то создаем для него каталог
	if err := os.MkdirAll(filepath.Dir(fi.Location), 0700); err != nil {
		err.(*os.PathError).Path = fi.Name
		return nil, err
	}

	// Перемещаем временный файл в этот каталог
	if err := os.Rename(tmpfile.Name(), fi.Location); err != nil {
		if _, ok := err.(*os.PathError); ok {
			err.(*os.PathError).Path = fi.Name
		}
		return nil, err
	}

	// Возвращаем информацию о созданном файле
	return fi, nil
}

// Open открывает файл из каталога.
func (s *Store) Open(name string) (*os.File, error) {
	// Полное имя для доступа к файлу
	fullName := s.GetFullName(name)
	if fullName == "" {
		return nil, os.ErrNotExist
	}

	// Открываем файл
	file, err := os.Open(fullName)
	if err != nil {
		err.(*os.PathError).Path = name
		return nil, err
	}

	// Получаем информацию о файле и проверяем, что это не каталог
	fi, err := file.Stat()
	if err != nil {
		file.Close()
		err.(*os.PathError).Path = name
		return nil, err
	}

	// Возвращаем ошибку, если это каталог, а не файл
	if fi.IsDir() {
		file.Close()
		return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrPermission}
	}

	// Обновляем время доступа к файлу
	now := time.Now()
	os.Chtimes(fullName, now, now)

	return file, nil // Возвращаем открытый файл
}

// Remove удаляет файл из хранилища.
func (s *Store) Remove(name string) error {
	mu := s.getMutex(name)
	mu.Lock()
	defer mu.Unlock()

	// Полное имя для доступа к файлу
	fullName := s.GetFullName(name)
	if fullName == "" {
		return os.ErrNotExist
	}

	if err := os.Remove(fullName); err != nil {
		err.(*os.PathError).Path = name
		return err
	}

	// Пытаемся удалить пустые каталоги, если они образовались
	for range 2 {
		fullName = filepath.Dir(fullName)
		if err := os.Remove(fullName); err != nil {
			break // Если не получилось, значит каталог не пустой
		}
	}

	return nil
}

// Clean удаляет старые файлы, к которым не обращались больше заданного времени.
func (s *Store) Clean(lifetime time.Duration) error {
	// Удаляем вообще все файлы, если время жизни не задано
	if lifetime <= 0 {
		files, err := filepath.Glob(filepath.Join(s.dir, "*"))
		if err != nil {
			return err
		}
		for _, file := range files {
			if err := os.RemoveAll(file); err != nil {
				return err
			}
		}
	}

	// Вычисляем крайнюю дату валидности файлов
	valid := time.Now().Add(-lifetime)

	err := filepath.Walk(s.dir,
		func(filename string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			// Не удаляем каталоги и новые файлы
			if info.IsDir() || info.ModTime().After(valid) {
				return nil
			}

			// Удаляем старый файл
			if err = os.Remove(filename); err != nil {
				return nil // Ошибку удаления игнорируем
			}

			// Пытаемся удалить пустые каталоги
			for range 2 {
				filename = filepath.Dir(filename)
				if err = os.Remove(filename); err != nil {
					break // Каталог не пустой
				}
			}

			return nil
		},
	)

	if os.IsNotExist(err) {
		return nil // Игнорируем ошибку, что файл не существует
	}

	return err
}

// getMutex
func (s *Store) getMutex(name string) *sync.Mutex {
	s.mutexes.Do(func() { s.mutexes.m = make(map[string]*sync.Mutex) })

	s.mutexes.Lock()
	mu, ok := s.mutexes.m[name]
	if !ok {
		mu = &sync.Mutex{}
		s.mutexes.m[name] = mu
	}
	s.mutexes.Unlock()

	return mu
}

// GetFullName возвращает полный путь к файлу в хранилище.
func (s *Store) GetFullName(name string) string {
	if len(name) < 27 {
		return ""
	}
	return filepath.Join(s.dir, name[:1], name[1:3], name[3:])
}

// IsExists проверяет: существует ли файл в хранилище?
func (s *Store) IsExists(name string) bool {
	fullName := s.GetFullName(name)
	if fullName == "" {
		return false
	}

	fi, err := os.Stat(fullName)
	if os.IsNotExist(err) {
		return false
	}

	return !fi.IsDir()
}
