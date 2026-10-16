# filestore <!-- omit in toc -->

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/tenrok/filestore)

Универсальное файловое хранилище с абстракцией над локальной файловой системой и удалёнными S3-совместимыми бэкендами. Реализует `http.FileSystem`, поддерживает постраничный листинг и
контент-адресуемые имена файлов (дедупликация).

Основан на [mdigger/filestore](https://github.com/mdigger/filestore).

## Возможности

- Единый API для локального и удалённого хранилищ.
- Совместимость с `net/http`: `http.FileSystem`, HTTP Range, `If-Modified-Since`.
- Контент-адресуемые имена `Base32(CRC32_BE || MD5)` — дедупликация по содержимому.
- Постраничный листинг `List(ctx, prefix, limit, token)`.
- Стриминговые операции: S3 multipart при загрузке, range-запросы при чтении.
- Плагинные бэкенды по схеме строки подключения (`s3`; `minio` — устаревший алиас).

## Содержание

- [Возможности](#возможности)
- [Содержание](#содержание)
- [Установка](#установка)
- [Архитектура](#архитектура)
- [Жизненный цикл](#жизненный-цикл)
  - [Запись](#запись)
  - [Чтение](#чтение)
- [Быстрый старт](#быстрый-старт)
  - [Только локально](#только-локально)
  - [Локально + S3](#локально--s3)
- [API](#api)
  - [`NewHttpFS`](#newhttpfs)
  - [Опции](#опции)
  - [`List`](#list)
- [Формирование имени файла](#формирование-имени-файла)
  - [Алгоритм](#алгоритм)
  - [Реализация](#реализация)
  - [Дедупликация](#дедупликация)
- [Постраничный листинг](#постраничный-листинг)
- [Интеграция с net/http](#интеграция-с-nethttp)
- [Удалённое хранилище (S3 / MinIO)](#удалённое-хранилище-s3--minio)
  - [Строка подключения](#строка-подключения)
- [Обработка ошибок](#обработка-ошибок)
- [Тестирование](#тестирование)
- [Ограничения](#ограничения)
- [Лицензия](#лицензия)

## Установка

```bash
go get github.com/tenrok/filestore
```

Для S3-бэкенда:

```bash
go get github.com/aws/aws-sdk-go-v2
go get github.com/aws/aws-sdk-go-v2/config
go get github.com/aws/aws-sdk-go-v2/credentials
go get github.com/aws/aws-sdk-go-v2/feature/s3/manager
go get github.com/aws/aws-sdk-go-v2/service/s3
go get github.com/aws/smithy-go
```

## Архитектура

```
filestore/
├── HttpFS            — фасад, реализует http.FileSystem
├── LocalStorage      — локальная ФС; рабочая область
└── remote/
    ├── Storage       — интерфейс удалённого хранилища
    ├── FileInfo      — метаданные
    ├── ListResult    — страница листинга (Files + NextToken)
    ├── Uploader      — потоковая загрузка
    └── s3storage/    — реализация на aws-sdk-go-v2 (S3, MinIO, Ceph)
```

`minio://` — устаревший алиас, обрабатывается тем же `s3storage`. MinIO полностью S3-совместим, поэтому отдельная реализация больше не нужна.

`HttpFS` — точка входа. Все операции идут через него, а он уже решает, куда обратиться:

| Операция            | Без `remote`   | С `remote`                                    |
|---------------------|----------------|-----------------------------------------------|
| `Create` / `Write`  | `LocalStorage` | `LocalStorage` (временный файл + хеши)        |
| `Close`             | переименование | + загрузка в remote, удаление локальной копии |
| `Open`              | `LocalStorage` | **напрямую из `remote`**                      |
| `Remove`            | `LocalStorage` | `LocalStorage` + `remote`                     |
| `List`              | `LocalStorage` | `remote`                                      |
| `Stat` / `IsExists` | `LocalStorage` | `remote`                                      |

## Жизненный цикл

### Запись

`LocalStorage` при подключённом `remote` — **транзитная зона**: файл пишется во временный локальный файл, а после `Close` уезжает в удалённое хранилище и удаляется локально.

```
Client ── Create ──► HttpFS ── Create ──► LocalStorage (.tmp/xxx)
                                              │
Client ── Write(p) × N ─────────────────────► │  (temp + crc32 + md5)
                                              │
Client ── Close ────────────────────────────► │
                                              ├─ finalize: name = Base32(crc32 || md5)
                                              ├─ rename temp → <root>/<name>
                                              ├─ if remote: remote.Upload(name)
                                              └─ if remote: LocalStorage.Remove(name)
```

**Фазы:**

1. **`Create(name)`** — создаёт временный файл в `LocalStorage`, инициализирует потоковые вычислители `CRC32` и `MD5`. Аргумент `name` не используется как итоговое имя.
2. **`Write(p)` × N** — пишет данные одновременно во временный файл и в оба хешера (`io.MultiWriter`). Память не растёт.
3. **`Close()`** — финализирует хеши, формирует контент-адресуемое имя, переименовывает временный файл. Если подключён `remote` — загружает объект и удаляет локальную копию.

**Отмена.** `Create` реагирует на отмену `ctx` синхронно: при `ctx.Done()` временный файл удаляется, хешеры уничтожаются, висящих горутин не остаётся.

**Дедупликация.** Если файл с таким содержимым уже есть, `mtime` **не перезаписывается** — обновляется только `atime`. Это сохраняет корректность `If-Modified-Since` в HTTP.

### Чтение

```
Client ── Open ──► HttpFS
                    ├── remote != nil ──► remote.Open (напрямую)
                    └── remote == nil ──► LocalStorage.Open
```

При подключённом `remote` файл отдаётся напрямую из удалённого хранилища — без промежуточного скачивания в локальное.

`LocalStorage.Open` **сохраняет `mtime`** и обновляет только `atime`. Иначе `Last-Modified` в HTTP-ответе всегда был бы равен моменту чтения, что ломает кэширование и `If-Modified-Since`.

## Быстрый старт

### Только локально

```go
fs, err := filestore.NewHttpFS("/var/lib/files")
if err != nil {
    log.Fatal(err)
}
http.Handle("/files/", http.StripPrefix("/files", http.FileServer(fs)))
log.Fatal(http.ListenAndServe(":8080", nil))
```

### Локально + S3

```go
ctx := context.Background()

storage, err := remote.NewStorage(ctx, "s3://minioadmin:minioadmin@localhost:9000/uploads?secure=0")
if err != nil {
    log.Fatal(err)
}

fs, err := filestore.NewHttpFS("/tmp/staging", filestore.WithRemoteStorage(storage))
if err != nil {
    log.Fatal(err)
}

http.Handle("/files/", http.StripPrefix("/files", http.FileServer(fs)))
log.Fatal(http.ListenAndServe(":8080", nil))
```

## API

### `NewHttpFS`

```go
func NewHttpFS(root string, opts ...Option) (*HttpFS, error)
```

- `root` — локальный каталог (`LocalStorage`). Создаётся, если не существует.
- `opts` — опции.

Возвращаемый `*HttpFS` реализует стандартный `http.FileSystem`:

```go
type FileSystem interface {
    Open(name string) (File, error)
}

type File interface {
    io.Closer
    io.Reader
    io.Seeker
    Readdir(count int) ([]fs.FileInfo, error)
    Stat() (fs.FileInfo, error)
}
```

### Опции

- **`WithRemoteStorage(storage remote.Storage)`** — подключает удалённое хранилище как вторичное. Меняет поведение `Create`/`Close` (транзитная зона) и `Open`/`List` (делегирование в remote).
  Без этой опции `HttpFS` работает только с локальным каталогом.

### `List`

```go
func (f *HttpFS) List(ctx context.Context, prefix string, limit int, token string) (*remote.ListResult, error)
```

При подключённом `remote` делегируется в `remote.Storage.List`, иначе — в `LocalStorage.List` (рекурсивный обход, токен — offset).

## Формирование имени файла

Имя объекта формируется из содержимого, что даёт **дедупликацию**: одинаковые файлы получают одинаковое имя.

### Алгоритм

1. **CRC32** (полином IEEE) — 4 байта в **Big-Endian**.
2. **MD5** — 16 байт.
3. Конкатенация: `[4 байта CRC32][16 байт MD5]` = 20 байт.
4. Кодирование в **Base32** (стандартный алфавит, паддинг `=`).

Формула:

$$Name = Base32(CRC32_{BE}(File) \parallel MD5(File))$$

| Свойство            | Значение                                   |
|---------------------|--------------------------------------------|
| Вход                | 20 байт                                    |
| Выход               | 32 символа                                 |
| Алфавит             | `A–Z`, `2–7`, `=`                          |
| Детерминированность | Да                                         |
| Коллизии            | CRC32 — быстрый фильтр, MD5 — вторая линия |

### Реализация

```go
func ContentName(r io.Reader) (string, error) {
    crc := crc32.NewIEEE()
    md := md5.New()
    if _, err := io.Copy(io.MultiWriter(crc, md), r); err != nil {
        return "", err
    }
    var buf [20]byte
    binary.BigEndian.PutUint32(buf[0:4], crc.Sum32())
    copy(buf[4:20], md.Sum(nil))
    return base32.StdEncoding.EncodeToString(buf[:]), nil
}
```

Для `"hello, s3"` результат — `3CII3JDY4VFYO4UW2Y5D75XY3U62GEVO`.

### Дедупликация

При `Close()` перед загрузкой в `remote` вызывается `IsExists(name)`. Если объект уже есть — загрузка пропускается, временный файл удаляется.

## Постраничный листинг

```go
const pageSize = 100
var token string
for {
    page, err := fs.List(ctx, "", pageSize, token)
    if err != nil {
        log.Fatal(err)
    }
    for _, f := range page.Files {
        fmt.Printf("%-60s %10d  %s\n", f.Name(), f.Size(), f.ModTime().Format(time.RFC3339))
    }
    if page.NextToken == "" {
        break
    }
    token = page.NextToken
}
```

| Источник         | `limit ≤ 0`                           | `limit > max` |
|------------------|---------------------------------------|---------------|
| S3 (`s3storage`) | 100                                   | 1000          |
| `LocalStorage`   | без ограничения (все файлы от offset) | —             |

**Семантика `prefix` и имён:**

- `s3storage.List` возвращает имена **относительно префикса хранилища**. Это гарантирует, что `FileInfo.Name()` можно сразу передать в `Open`, `Stat` или `Remove` — без ручного срезания префикса.
- `LocalStorage.List` фильтрует по плоскому контент-адресуемому имени (32 символа Base32, без разделителей). Некорректный `prefix` (с `..` или абсолютный путь) отсекается защитой от path traversal.

**Токен `LocalStorage`** — неотрицательное целое (offset). Пустая строка — с начала. Отрицательное значение или не-число → ошибка `invalid token`.

Токены **не взаимозаменяемы** между бэкендами.

## Интеграция с net/http

`http.FileServer` — для мелких файлов и статики:

```go
http.Handle("/files/", http.StripPrefix("/files", http.FileServer(fs)))
```

`http.ServeContent` — для больших файлов и корректной работы с HTTP Range:

```go
f, err := fs.Open(strings.TrimPrefix(r.URL.Path, "/files/"))
if err != nil {
    http.NotFound(w, r)
    return
}
defer f.Close()

info, _ := f.Stat()
http.ServeContent(w, r, info.Name(), info.ModTime(), f)
```

`http.FileServer` проверяет `errors.Is(err, os.ErrNotExist)` для отдачи 404. Все бэкенды (`LocalStorage` и `s3storage`) маппят «не найдено» именно в `os.ErrNotExist`.
Прочие ошибки (в том числе `ErrIsDirectory`) уходят клиенту как 500 — это сигнал о поломке хранилища.

## Удалённое хранилище (S3 / MinIO)

Бэкенд `remote/s3storage` работает на `aws-sdk-go-v2` и поддерживает AWS S3, MinIO, Ceph RadosGW, Yandex Object Storage, DigitalOcean Spaces.

### Строка подключения

```
s3://ACCESS_KEY:SECRET_KEY@ENDPOINT[:PORT]/BUCKET[/PREFIX][?secure=1&region=REGION&token=TOKEN]
minio://...  (устаревший алиас, эквивалент s3://)
```

| Параметр                  | Описание                                                 |
|---------------------------|----------------------------------------------------------|
| `ACCESS_KEY`/`SECRET_KEY` | Если не заданы — default chain AWS (env, `~/.aws`, IAM). |
| `ENDPOINT`                | Хост S3 API.                                             |
| `BUCKET`                  | Имя бакета (обязательно).                                |
| `PREFIX`                  | Префикс для всех операций (опционально).                 |
| `secure=1`                | HTTPS вместо HTTP. По умолчанию HTTP.                    |
| `region`                  | Регион AWS (по умолчанию `us-east-1`).                   |
| `token`                   | Session token для временных кред (STS).                  |

> [!NOTE]
> **`minio://` — deprecated.** Схема поддерживается для обратной совместимости и полностью эквивалентна `s3://`. Каноническая scheme — `s3`.
> Алиас будет удалён в следующем мажорном релизе — новые подключения пишите через `s3://`.

Примеры:

```
# AWS S3
s3://AKIA...:wJal...@s3.eu-west-1.amazonaws.com/my-bucket?secure=1&region=eu-west-1

# MinIO локально
s3://minioadmin:minioadmin@localhost:9000/uploads

# MinIO с префиксом "user-42/files" и HTTPS
s3://minioadmin:minioadmin@minio.internal:9000/uploads/user-42/files?secure=1

# AWS без статических кред (IAM-роль инстанса)
s3://@s3.us-east-1.amazonaws.com/my-bucket?secure=1&region=us-east-1

# AWS с временными кредами (STS)
s3://AKIA...:wJal...@s3.eu-west-1.amazonaws.com/my-bucket?secure=1&region=eu-west-1&token=STS_TOKEN

# MinIO со session token (обратная совместимость со старым miniostorage)
minio://ACCESS:SECRET@minio.internal:9000/uploads?token=STS_TOKEN
```

Использование:

```go
import (
    "github.com/tenrok/filestore/remote"
    _ "github.com/tenrok/filestore/remote/s3storage"
)

storage, err := remote.NewStorage(ctx, "s3://minioadmin:minioadmin@localhost:9000/uploads?secure=0")
```

Возможности:

- Стриминговая загрузка через `manager.Uploader` (multipart).
- Стриминговое скачивание через range-запросы (память ≤ 4 МиБ на файл).
- `List` с `ContinuationToken`, лимит 1000 объектов на страницу.
- `IsExists` возвращает `(false, nil)` при отсутствии объекта.
- `Open`, `Stat` маппят «не найдено» в `os.ErrNotExist`.

## Обработка ошибок

Стандартный Go-подход: ошибки возвращаются как есть либо оборачиваются через `fmt.Errorf("...: %w", err)`.

```go
f, err := fs.Open("nope.txt")
if errors.Is(err, fs.ErrNotExist) {
    // 404
}

// По имени лежит директория — раскладка хранилища повреждена.
// Возникает только при внешнем вмешательстве в LocalStorage.
if errors.Is(err, filestore.ErrIsDirectory) {
    log.Printf("filestore: %v", err)
}

var apiErr smithy.APIError
if errors.As(err, &apiErr) {
    log.Printf("S3 code=%s message=%s", apiErr.ErrorCode(), apiErr.ErrorMessage())
}
```

`LocalStorage.IsExists` возвращает `(false, err)` для директории, а не `(false, nil)` — чтобы не маскировать аномалию под «файла нет».

## Тестирование

```bash
go test ./...
go test -race ./...
```

Регрессионные тесты на обратную совместимость с `miniostorage`:

```bash
go test ./remote/s3storage/ -run 'Minio|RoundTrip' -v
```

Регрессионные тесты на конкурентность и path traversal:

```bash
go test ./... -race -run 'Concurrent|Traversal|ContextCancel|List'
```

Интеграционные тесты требуют S3-совместимого сервиса:

```bash
docker run -d --name minio \
    -p 9000:9000 -p 9001:9001 \
    -e MINIO_ROOT_USER=minioadmin \
    -e MINIO_ROOT_PASSWORD=minioadmin \
    minio/minio server /data --console-address ":9001"

mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/uploads

S3_TEST_CONN="s3://minioadmin:minioadmin@localhost:9000/uploads?secure=0" \
    go test ./remote/s3storage/... -v
```

## Ограничения

1. **Токены пагинации** не совместимы между бэкендами.
2. **`LocalStorage.List`** сканирует дерево целиком перед пагинацией.
3. **`s3storage` использует `ctx` из `NewStorage`** для всех последующих вызовов — для разных таймаутов интерфейс `remote.Storage` придётся расширить.
4. **MD5/CRC32 не криптостойки** — см. раздел «Формирование имени файла».
5. **`Content-Type` при стриминге** не определяется автоматически — передавайте явно через `remote.WithContentType`.
6. **Промежуточные файлы** занимают место в `LocalStorage` в момент загрузки. Следите за свободным местом в `root`.
7. **Итоговое имя файла** отличается от аргумента `Create(name)`.
8. **`minio://` — устаревший алиас.** Будет удалён в следующем мажоре. Формат строки подключения совпадает с `s3://`, включая `token`.
9. **`LocalStorage` работает только с контент-адресуемыми именами** (32 символа Base32). Каталоги для раскладки создаются автоматически; `List` возвращает плоские имена без `/`.

## Лицензия

MIT
