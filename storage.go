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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tenrok/filestore/remote"
)

// tmpfileName используется в качестве имени временного файла при генерации ошибок
const tmpfileName = "<temporary file>"

// ContentNameLen — длина имени, которую выдаёт ContentName (Base32(20 байт) = 32 символа).
const ContentNameLen = 32

// ErrIsDirectory — файл по имени существует, но это директория.
// Возникает только при внешнем вмешательстве в content-addressed раскладку.
var ErrIsDirectory = errors.New("is a directory")

// LocalStorage описывает хранилище файлов.
type LocalStorage struct {
	rootDir string
	perm    os.FileMode

	mu    sync.Mutex
	locks map[string]*lockEntry
}

// lockEntry — мьютекс с подсчётом ссылок для одного имени.
type lockEntry struct {
	mu   sync.Mutex
	refs int
}

type LocalStorageOption func(*LocalStorage)

// WithPermissions устанавливает права доступа для создаваемых каталогов и файлов.
func WithPermissions(perm os.FileMode) LocalStorageOption {
	return func(s *LocalStorage) {
		s.perm = perm
	}
}

// FileInfo описывает информацию о сохранённом файле.
type FileInfo struct {
	Path     string // Полный путь внутри хранилища
	Name     string // Уникальное имя файла
	Mimetype string
	Size     int64
	CRC32    uint32
	MD5      string
}

// NewLocalStorage открывает и возвращает хранилище файлов.
func NewLocalStorage(rootDir string, opts ...LocalStorageOption) (*LocalStorage, error) {
	s := &LocalStorage{
		perm:  0700,
		locks: make(map[string]*lockEntry),
	}
	s.rootDir = rootDir
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
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

// acquire блокирует имя и возвращает функцию освобождения.
// Мьютекс удаляется из map только когда все ожидающие освободят его.
func (s *LocalStorage) acquire(name string) func() {
	s.mu.Lock()
	e, ok := s.locks[name]
	if !ok {
		e = &lockEntry{}
		s.locks[name] = e
	}
	e.refs++
	s.mu.Unlock()

	e.mu.Lock()
	return func() {
		e.mu.Unlock()
		s.mu.Lock()
		e.refs--
		if e.refs == 0 {
			delete(s.locks, name)
		}
		s.mu.Unlock()
	}
}

// safePath проверяет, что путь не выходит за пределы rootDir.
// Пустой путь возвращает сам rootDir (пригодно для List с пустым prefix).
func (s *LocalStorage) safePath(subPath string) (string, error) {
	clean := filepath.Clean(strings.TrimLeft(subPath, "/"))
	if clean == "." || clean == "" {
		return s.rootDir, nil
	}
	if clean == ".." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("%w: path traversal attempt", os.ErrPermission)
	}

	full := filepath.Join(s.rootDir, clean)
	abs, err := filepath.Abs(full)
	if err != nil {
		return "", err
	}
	if abs != s.rootDir && !strings.HasPrefix(abs, s.rootDir+string(os.PathSeparator)) {
		return "", fmt.Errorf("%w: path traversal attempt", os.ErrPermission)
	}
	return abs, nil
}

// Create сохраняет поток в хранилище и возвращает FileInfo.
// Имя файла — Base32(CRC32_BE || MD5), т.е. content-addressed.
// При отмене контекста возвращается ctx.Err() без гонки с горутиной записи.
func (s *LocalStorage) Create(ctx context.Context, r io.Reader) (*FileInfo, error) {
	if r == nil {
		return nil, errors.New("reader is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Создаём временный файл в корневом каталоге
	tmpfile, err := os.CreateTemp(s.rootDir, "~tmp")
	if err != nil {
		return nil, s.wrapPathError(err, tmpfileName)
	}
	tmpName := tmpfile.Name()
	cleanup := func() {
		_ = tmpfile.Close()
		_ = os.Remove(tmpName)
	}

	// Копируем содержимое во временный файл
	bufferReader := bufio.NewReaderSize(r, 4<<10)
	if err := ctx.Err(); err != nil {
		cleanup()
		return nil, err
	}

	// Пытаемся определить MIME-тип содержимого
	data, err := bufferReader.Peek(512)
	if err != nil && !errors.Is(err, io.EOF) {
		cleanup()
		return nil, s.wrapPathError(err, tmpfileName)
	}
	mimetype := http.DetectContentType(data)

	// Одновременно с сохранением в файл считаем две хеш-суммы
	hashCRC32 := crc32.NewIEEE()
	hashMD5 := md5.New()
	multiWriter := io.MultiWriter(tmpfile, hashCRC32, hashMD5)

	size, err := copyWithContext(ctx, multiWriter, bufferReader)
	if err != nil {
		cleanup()
		return nil, s.wrapPathError(err, tmpfileName)
	}

	if err := tmpfile.Close(); err != nil {
		_ = os.Remove(tmpName)
		return nil, s.wrapPathError(err, tmpfileName)
	}

	// Формируем информацию о файле
	sumMD5 := hashMD5.Sum(nil)
	var nameBuf [20]byte
	copy(nameBuf[0:4], hashCRC32.Sum(nil))
	copy(nameBuf[4:20], sumMD5)
	name := base32.StdEncoding.EncodeToString(nameBuf[:])

	fi := &FileInfo{
		Path:     s.GetRelativePath(name),
		Name:     name,
		Mimetype: mimetype,
		Size:     size,
		CRC32:    hashCRC32.Sum32(),
		MD5:      hex.EncodeToString(sumMD5),
	}

	fullPath, err := s.safePath(fi.Path)
	if err != nil {
		_ = os.Remove(tmpName)
		return nil, err
	}

	// Дедупликация: если файл уже есть — только обновляем atime, mtime сохраняем.
	if existing, statErr := os.Stat(fullPath); statErr == nil {
		_ = os.Remove(tmpName)
		_ = os.Chtimes(fullPath, time.Now(), existing.ModTime())
		return fi, nil
	} else if !os.IsNotExist(statErr) {
		// Другая ошибка (например, permission denied) – не можем перезаписать
		_ = os.Remove(tmpName)
		return nil, s.wrapPathError(statErr, name)
	}

	// Если такого файла нет, то создаем для него каталоги
	if err := os.MkdirAll(filepath.Dir(fullPath), s.perm); err != nil {
		_ = os.Remove(tmpName)
		return nil, s.wrapPathError(err, name)
	}

	// Перемещаем временный файл
	if err := os.Rename(tmpName, fullPath); err != nil {
		_ = os.Remove(tmpName)
		return nil, s.wrapPathError(err, name)
	}
	return fi, nil
}

// copyWithContext — синхронная замена io.Copy, реагирует на отмену контекста.
func copyWithContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	buf := make([]byte, 32*1024)
	var written int64
	for {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		n, rerr := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return written, werr
			}
			written += int64(n)
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				return written, nil
			}
			return written, rerr
		}
	}
}

// Open открывает файл из хранилища. mtime не меняется; обновляется только atime.
func (s *LocalStorage) Open(name string) (*os.File, error) {
	// Полное имя для доступа к файлу
	fullPath, err := s.GetFullPath(name)
	if err != nil {
		return nil, err
	}

	// Открываем файл
	file, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, os.ErrNotExist
		}
		return nil, s.wrapPathError(err, name)
	}

	// Получаем информацию о файле и проверяем, что это не каталог
	fi, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, s.wrapPathError(err, name)
	}

	// Возвращаем ошибку, если это каталог, а не файл
	if fi.IsDir() {
		_ = file.Close()
		return nil, &os.PathError{Op: "open", Path: name, Err: ErrIsDirectory}
	}

	// Обновляем atime с сохранением mtime (ошибку игнорируем, это не критично).
	_ = os.Chtimes(fullPath, time.Now(), fi.ModTime())
	return file, nil
}

// Remove удаляет файл из хранилища.
func (s *LocalStorage) Remove(name string) error {
	release := s.acquire(name)
	defer release()

	fullPath, err := s.GetFullPath(name)
	if err != nil {
		return err
	}
	if err := os.Remove(fullPath); err != nil {
		if os.IsNotExist(err) {
			return os.ErrNotExist
		}
		return s.wrapPathError(err, name)
	}
	s.removeEmptyParents(fullPath)
	return nil
}

// removeEmptyParents поднимается вверх по дереву, удаляя пустые каталоги до rootDir.
func (s *LocalStorage) removeEmptyParents(path string) {
	dir := filepath.Dir(path)
	for dir != s.rootDir && dir != "." && dir != string(os.PathSeparator) {
		if err := os.Remove(dir); err != nil {
			return
		}
		dir = filepath.Dir(dir)
	}
}

// Clean удаляет файлы, к которым не обращались дольше lifetime.
// lifetime <= 0 — удаляет всё содержимое rootDir.
func (s *LocalStorage) Clean(ctx context.Context, lifetime time.Duration) error {
	if lifetime <= 0 {
		entries, err := os.ReadDir(s.rootDir)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			_ = os.RemoveAll(filepath.Join(s.rootDir, entry.Name()))
		}
		return nil
	}

	valid := time.Now().Add(-lifetime)
	err := filepath.Walk(s.rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if info.IsDir() || info.ModTime().After(valid) {
			return nil
		}
		rel, relErr := filepath.Rel(s.rootDir, path)
		if relErr != nil {
			return nil
		}
		flat := strings.ReplaceAll(rel, string(os.PathSeparator), "")

		release := s.acquire(flat)
		defer release()

		if err := os.Remove(path); err != nil {
			return nil
		}
		s.removeEmptyParents(path)
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// wrapPathError безопасно заменяет путь в *os.PathError.
func (s *LocalStorage) wrapPathError(err error, name string) error {
	var pe *os.PathError
	if errors.As(err, &pe) {
		return &os.PathError{Op: pe.Op, Path: name, Err: pe.Err}
	}
	return err
}

// GetRelativePath возвращает путь внутри хранилища: A/BC/DEF...
func (s *LocalStorage) GetRelativePath(name string) string {
	name = strings.TrimLeft(name, "/")
	if len(name) < ContentNameLen {
		return ""
	}
	return filepath.Join(name[:1], name[1:3], name[3:])
}

// GetFullPath возвращает абсолютный путь к файлу.
func (s *LocalStorage) GetFullPath(name string) (string, error) {
	rel := s.GetRelativePath(name)
	if rel == "" {
		return "", fmt.Errorf("invalid file name: %s", name)
	}
	return s.safePath(rel)
}

// IsExists проверяет существование файла.
// (false, nil) — файла нет.
// (false, err) — ошибка доступа/IO либо по имени лежит директория.
func (s *LocalStorage) IsExists(name string) (bool, error) {
	fullPath, err := s.GetFullPath(name)
	if err != nil {
		return false, err
	}

	fi, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}

	if fi.IsDir() {
		return false, &os.PathError{Op: "stat", Path: name, Err: ErrIsDirectory}
	}
	return true, nil
}

// List возвращает страницу файлов.
// prefix — префикс контент-адресуемого имени; пустая строка — все файлы.
// limit <= 0 — без ограничения (возвращает всё, начиная с offset).
// Токен — числовой offset.
func (s *LocalStorage) List(ctx context.Context, prefix string, limit int, token string) (*remote.ListResult, error) {
	var allFiles []remote.FileInfo

	walkErr := filepath.Walk(s.rootDir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, relErr := filepath.Rel(s.rootDir, p)
		if relErr != nil {
			return nil
		}
		flat := strings.ReplaceAll(rel, string(os.PathSeparator), "")
		if prefix != "" && !strings.HasPrefix(flat, prefix) {
			return nil
		}
		allFiles = append(allFiles, newLocalFileInfo(flat, info))
		return nil
	})
	if walkErr != nil && !os.IsNotExist(walkErr) {
		return nil, walkErr
	}

	offset := 0
	if token != "" {
		v, perr := strconv.Atoi(token)
		if perr != nil || v < 0 {
			return nil, fmt.Errorf("invalid token: %q", token)
		}
		offset = v
	}
	if offset > len(allFiles) {
		offset = len(allFiles)
	}

	if limit <= 0 {
		limit = len(allFiles) - offset
	}
	end := offset + limit
	if end > len(allFiles) {
		end = len(allFiles)
	}

	nextToken := ""
	if end < len(allFiles) {
		nextToken = strconv.Itoa(end)
	}

	return &remote.ListResult{
		Files:     allFiles[offset:end],
		NextToken: nextToken,
	}, nil
}
