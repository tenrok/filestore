package filestore

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/base32"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// tmpfileName используется в качестве имени временного файла при генерации ошибок
const tmpfileName = "<temporary file>"

// минимальная длина имени файла, необходимая для разбиения на подкаталоги
const minNameLen = 27

// LocalStorage описывает хранилище файлов.
type LocalStorage struct {
	rootDir string
	perm    os.FileMode

	// защита для map мьютексов
	mu        sync.Mutex
	once      sync.Once
	fileMutex map[string]*sync.Mutex // мьютекс на имя файла
}

type LocalStorageOption func(*LocalStorage)

// WithPermissions устанавливает права доступа для создаваемых каталогов и файлов.
func WithPermissions(perm os.FileMode) LocalStorageOption {
	return func(s *LocalStorage) {
		s.perm = perm
	}
}

// FileInfo описывает информацию о сохраненном файле.
type FileInfo struct {
	Path     string // полный путь внутри хранилища
	Name     string // уникальное имя файла
	Mimetype string
	Size     int64
	CRC32    uint32
	MD5      string
}

// NewLocalStorage открывает и возвращает хранилище файлов.
func NewLocalStorage(rootDir string, opts ...LocalStorageOption) (*LocalStorage, error) {
	s := &LocalStorage{}
	s.rootDir = rootDir
	s.perm = 0700
	s.fileMutex = make(map[string]*sync.Mutex)

	for _, opt := range opts {
		opt(s)
	}

	// Очищаем путь и делаем его абсолютным для корректной проверки безопасности
	absRoot, err := filepath.Abs(s.rootDir)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path for rootDir: %w", err)
	}
	s.rootDir = absRoot

	// Создаём каталог, если он ещё не создан
	if err := os.MkdirAll(s.rootDir, s.perm); err != nil {
		return nil, err
	}

	return s, nil
}

// getMutex возвращает мьютекс для имени файла, создавая его при необходимости.
func (s *LocalStorage) getMutex(name string) *sync.Mutex {
	s.once.Do(func() {
		// инициализация уже выполнена в NewLocalStorage, но оставляем для безопасности
		if s.fileMutex == nil {
			s.fileMutex = make(map[string]*sync.Mutex)
		}
	})

	s.mu.Lock()
	defer s.mu.Unlock()

	mu, ok := s.fileMutex[name]
	if !ok {
		mu = &sync.Mutex{}
		s.fileMutex[name] = mu
	}
	return mu
}

// releaseMutex удаляет мьютекс из map после использования (вызывать после Unlock).
func (s *LocalStorage) releaseMutex(name string) {
	s.mu.Lock()
	delete(s.fileMutex, name)
	s.mu.Unlock()
}

// safePath проверяет, что путь не выходит за пределы rootDir, и возвращает очищенный путь.
func (s *LocalStorage) safePath(subPath string) (string, error) {
	// Убираем начальные разделители и ".." попытки
	clean := filepath.Clean(strings.TrimPrefix(subPath, "/"))

	// Запрещаем пустые и слишком короткие имена
	if clean == "" || clean == "." || clean == ".." {
		return "", os.ErrNotExist
	}

	full := filepath.Join(s.rootDir, clean)

	// Проверяем, что итоговый путь всё ещё внутри rootDir
	absFull, err := filepath.Abs(full)
	if err != nil {
		return "", err
	}

	if !strings.HasPrefix(absFull, s.rootDir+string(os.PathSeparator)) && absFull != s.rootDir {
		return "", fmt.Errorf("%w: path traversal attempt", os.ErrPermission)
	}

	return absFull, nil
}

// Create сохраняет файл в хранилище. В качестве имени файла используется комбинация из двух хешей.
func (s *LocalStorage) Create(ctx context.Context, r io.Reader) (*FileInfo, error) {
	if r == nil {
		return nil, errors.New("reader is nil")
	}

	// Создаём временный файл в корневом каталоге
	tmpfile, err := os.CreateTemp(s.rootDir, "~tmp")
	if err != nil {
		return nil, s.wrapPathError(err, tmpfileName)
	}
	defer func() {
		tmpfile.Close()
		os.Remove(tmpfile.Name())
	}()

	// Копируем содержимое во временный файл
	bufferReader := bufio.NewReaderSize(r, 4<<10)

	// Пытаемся определить MIME-тип содержимого
	data, err := bufferReader.Peek(512)
	if err != nil && err != io.EOF {
		return nil, s.wrapPathError(err, tmpfileName)
	}
	mimetype := http.DetectContentType(data)

	// Одновременно с сохранением в файл считаем две хеш-суммы
	hashCRC32, hashMD5 := crc32.NewIEEE(), md5.New()
	multiWriter := io.MultiWriter(tmpfile, hashCRC32, hashMD5)

	type copyResult struct {
		size int64
		err  error
	}
	done := make(chan copyResult, 1)
	go func() {
		size, err := bufferReader.WriteTo(multiWriter)
		done <- copyResult{size, err}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-done:
		if res.err != nil {
			return nil, s.wrapPathError(res.err, tmpfileName)
		}
		// Формируем информацию о файле
		sumMD5 := hashMD5.Sum(nil)
		name := base32.StdEncoding.EncodeToString(append(hashCRC32.Sum(nil), sumMD5...))
		fi := &FileInfo{
			Path:     s.GetRelativePath(name),
			Name:     name,
			Mimetype: mimetype,
			Size:     res.size,
			CRC32:    hashCRC32.Sum32(),
			MD5:      hex.EncodeToString(sumMD5),
		}

		// Закрываем временный файл
		if err := tmpfile.Close(); err != nil {
			return nil, s.wrapPathError(err, tmpfileName)
		}

		fullPath, err := s.safePath(fi.Path)
		if err != nil {
			return nil, err
		}

		// Если файл уже существует, то просто обновляем его время создания
		now := time.Now()
		if err := os.Chtimes(fullPath, now, now); err == nil {
			return fi, nil
		} else if !os.IsNotExist(err) {
			// Другая ошибка (например, permission denied) – не можем перезаписать
			return nil, s.wrapPathError(err, name)
		}

		// Если такого файла нет, то создаем для него каталоги
		if err := os.MkdirAll(filepath.Dir(fullPath), s.perm); err != nil {
			return nil, s.wrapPathError(err, name)
		}

		// Перемещаем временный файл
		if err := os.Rename(tmpfile.Name(), fullPath); err != nil {
			return nil, s.wrapPathError(err, name)
		}

		return fi, nil
	}
}

// Open открывает файл из хранилища.
func (s *LocalStorage) Open(name string) (*os.File, error) {
	// Полное имя для доступа к файлу
	fullPath, err := s.GetFullPath(name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, os.ErrNotExist
		}
		return nil, err
	}

	// Открываем файл
	file, err := os.Open(fullPath)
	if err != nil {
		return nil, s.wrapPathError(err, name)
	}

	// Получаем информацию о файле и проверяем, что это не каталог
	fi, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, s.wrapPathError(err, name)
	}

	// Возвращаем ошибку, если это каталог, а не файл
	if fi.IsDir() {
		file.Close()
		return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrPermission}
	}

	// Обновляем время доступа (ошибку игнорируем, это не критично)
	now := time.Now()
	_ = os.Chtimes(fullPath, now, now)
	return file, nil
}

// Remove удаляет файл из хранилища.
func (s *LocalStorage) Remove(name string) error {
	mu := s.getMutex(name)
	mu.Lock()
	defer func() {
		mu.Unlock()
		s.releaseMutex(name)
	}()

	fullPath, err := s.GetFullPath(name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return os.ErrNotExist
		}
		return err
	}

	if err := os.Remove(fullPath); err != nil {
		return s.wrapPathError(err, name)
	}

	// Удаляем пустые родительские каталоги, но не выше rootDir
	s.removeEmptyParents(fullPath)
	return nil
}

// removeEmptyParents поднимается вверх по дереву каталогов, удаляя пустые,
// пока не дойдёт до rootDir или не встретит непустой каталог.
func (s *LocalStorage) removeEmptyParents(path string) {
	dir := filepath.Dir(path)
	for dir != s.rootDir && dir != "." && dir != "/" {
		// Пытаемся удалить каталог
		if err := os.Remove(dir); err != nil {
			break // не пустой или ошибка прав
		}
		dir = filepath.Dir(dir)
	}
}

// Clean удаляет старые файлы, к которым не обращались больше заданного времени.
// Если lifetime <= 0, удаляет все файлы.
func (s *LocalStorage) Clean(ctx context.Context, lifetime time.Duration) error {
	if lifetime <= 0 {
		// Удаляем всё содержимое rootDir, но не саму директорию
		entries, err := os.ReadDir(s.rootDir)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if err := os.RemoveAll(filepath.Join(s.rootDir, entry.Name())); err != nil {
				// Логируем, но продолжаем (по аналогии с оригиналом)
				continue
			}
		}
		return nil
	}

	valid := time.Now().Add(-lifetime)
	err := filepath.Walk(s.rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // игнорируем ошибки доступа к файлу
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if info.IsDir() || info.ModTime().After(valid) {
			return nil
		}

		// Получаем имя файла относительно rootDir
		rel, err := filepath.Rel(s.rootDir, path)
		if err != nil {
			return nil
		}

		// Имя файла в нашей терминологии — это путь без rootDir
		fileName := strings.ReplaceAll(rel, string(os.PathSeparator), "") // упрощённо, но для блокировки нужно исходное имя

		// Блокируем файл на время удаления
		mu := s.getMutex(fileName)
		mu.Lock()
		defer mu.Unlock()

		if err := os.Remove(path); err != nil {
			return nil // игнорируем ошибку удаления
		}
		s.removeEmptyParents(path)
		return nil
	})

	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// wrapPathError безопасно заменяет путь в ошибке, если она является *os.PathError.
func (s *LocalStorage) wrapPathError(err error, name string) error {
	if pe, ok := err.(*os.PathError); ok {
		pe.Path = name
		return pe
	}
	return err
}

// GetRelativePath возвращает относительный путь к файлу в хранилище (без учёта rootDir).
func (s *LocalStorage) GetRelativePath(name string) string {
	name = strings.TrimPrefix(name, "/")
	if len(name) < minNameLen {
		return ""
	}
	// Раскладываем по подкаталогам: первая буква, вторая+третья, остальное
	return filepath.Join(name[:1], name[1:3], name[3:])
}

// GetFullPath возвращает полный путь к файлу в хранилище.
func (s *LocalStorage) GetFullPath(name string) (string, error) {
	relPath := s.GetRelativePath(name)
	if relPath == "" {
		return "", fmt.Errorf("invalid file name: %s", name)
	}
	return s.safePath(relPath)
}

// IsExists проверяет существование файла.
func (s *LocalStorage) IsExists(name string) (bool, error) {
	fullPath, err := s.GetFullPath(name)
	if err != nil {
		return false, err
	}

	fi, err := os.Stat(fullPath)
	if err != nil {
		return false, err
	}

	if fi.IsDir() {
		return false, fmt.Errorf("The specified file is a directory")
	}

	return true, nil
}
