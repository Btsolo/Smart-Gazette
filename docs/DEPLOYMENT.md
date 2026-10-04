# Deploying Smart Gazette — notes for whoever sets up CI/CD

Smart Gazette turns Kenya Gazette PDFs into structured records and readable
articles. It is a Java 17 / Spring Boot 3.1.5 web app with PostgreSQL, but it
also **calls external programs** to read PDFs — the server needs more than a
JRE. This page lists everything the deployment depends on.

## 1. What the deployed version does (and does not do)

- **Read-only public site.** `admin.enabled` defaults to `false`: every page
  that uploads, edits, deletes or posts (`/admin/**`, `/add`, `/edit/**`,
  `/update`, `/delete/**`, `/login`, `/ifttt-post/**`) answers 404. The admin
  pages have no real login yet (Spring Security is the next roadmap stage),
  so they stay off on any server. Never set `admin.enabled=true` on a
  public server.
- **New gazettes arrive by themselves**: every evening at 22:00
  Africa/Nairobi (`scraper.cron`, default `0 0 22 * * *` - a notice published
  during the day is on the site the same night) the scraper checks the newest
  5 gazettes on Kenya Law (`scraper.max-per-run`), downloads each one not
  processed yet and processes them one at a time (AI calls to Groq / Gemini).
  It names itself (`scraper.user-agent`) and waits 5 s between requests
  (`scraper.pause-seconds`, Kenya Law's crawl delay). A gazette whose
  processing failed is tried again the next night. A staging copy can set
  `scraper.enabled=false`.
  **Authorisation:** Kenya Law's terms of use restrict scraping; the owner is
  asking Kenya Law for permission / a data feed.
- **No social posting**: `social.posting.enabled` defaults to `false`; no IFTTT
  setting is needed. (An old commit on `main` contains the owner's real IFTTT
  webhook URL; the owner should delete or regenerate it in IFTTT.)
- **Database password**: new and strong for production, stored as a secret
  (the local development password appears in old commits).

## 2. Branches and how releases work

- `main` — what is deployed (production).
- `develop` — finished work waiting for release.
- `feature/*` — work in progress, merged into `develop` by pull request.
- A release = a pull request `develop` → `main`; CI deploys `main`.

Suggested GitHub settings: protect `main` (pull request required, CI must
pass, no force-push). Give the CI/CD engineer **Write** access (not Admin)
— Settings → Collaborators. All secrets go in GitHub Actions secrets / the
host's secret store, never in the repository.

**Blue-green** is a separate idea from the branches: two identical
production environments (blue = live, green = new version). Deploy to green,
check it, then switch traffic; switch back if something is wrong. It needs a
proxy / load balancer and a database both versions can use (Hibernate
`ddl-auto=update` only adds columns, so a new version and the old one can
share the database for a short switch-over). A simpler first step is a
**staging** environment deployed from `develop` and production from `main`.

## 3. What the server needs

| component | version | used for |
|---|---|---|
| Java (JRE/JDK) | 17 | the app (`./mvnw -B package` → jar) |
| PostgreSQL | 14+ | all data (tables created by Hibernate, `ddl-auto=update`) |
| Node.js | 18+ | `tools/inspect_positions.js` (the PDF text extractor); run `npm ci` in `tools/` — installs `@firecrawl/pdf-inspector` 1.15.0 |
| poppler-utils | 22+ | `pdftocairo` (ruling lines, figure positions), `pdftoppm` (page images), `pdfimages` (scan detection) |
| Tesseract OCR | 5.x with `eng` data | scanned gazettes; fallback for figure text |
| PP-OCRv6 models | 31 MB, 3 files | text on maps / charts (`models/ppocr/`, not in git — see §5) |
| ONNX Runtime | bundled in the jar (1.20.0, Linux x64 / Windows x64) | runs the PP-OCR models; needs nothing installed |

Disk: uploaded PDFs go to `storage/gazettes/` (relative to the working
directory) and figure images to `figures.dir`. **Both must be persistent
volumes** — the database points at these files.

Memory: give the JVM at least 2 GB (`-Xmx2g`); PDF pages and OCR models are
held in memory while a gazette is processed. Processing is a background job:
a normal issue takes minutes, a large map issue up to ~30 minutes.

Example container (Debian-based):

```dockerfile
FROM eclipse-temurin:17-jre
RUN apt-get update && apt-get install -y --no-install-recommends \
      nodejs npm poppler-utils tesseract-ocr tesseract-ocr-eng \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY target/*.jar app.jar
COPY tools/ tools/
RUN cd tools && npm ci --omit=dev
RUN apt-get update && apt-get install -y --no-install-recommends curl && sh tools/ocr/fetch_models.sh /app/models/ppocr && rm -rf /var/lib/apt/lists/*
VOLUME ["/app/storage", "/data/figures"]
EXPOSE 8081
ENTRYPOINT ["java", "-Xmx2g", "-jar", "/app/app.jar"]
```

## 4. Configuration

`src/main/resources/application.properties` is **gitignored** (it holds
secrets). `application.properties.example` lists every setting. On a server,
pass them as environment variables (Spring maps `GROQ_API_KEY` →
`groq.api.key`, `SPRING_DATASOURCE_PASSWORD` → `spring.datasource.password`)
or mount a properties file.

| setting | secret? | production value |
|---|---|---|
| `spring.datasource.url` / `username` / `password` | password: yes | the production database |
| `groq.api.key`, `gemini.api.key` | yes | the owner's keys |
| `admin.enabled` | no | leave out (= `false`, read-only) |
| `scraper.enabled`, `scraper.cron` | no | `true` / `0 0 22 * * *` in production; `scraper.enabled=false` in staging if wanted |
| `social.posting.enabled`, `ifttt.webhook.url` | url: yes | leave out (no posting yet) |
| `extraction.engine` | no | **`inspector`** (the code default is `legacy`) |
| `extraction.inspector.script` | no | absolute path to `tools/inspect_positions.js` (the default `tools/inspect.js` is the old extractor) |
| `extraction.scan.tesseract` / `pdftoppm` / `pdfimages` | no | defaults work when the tools are on the PATH |
| `figures.dir` | no | a persistent volume, e.g. `/data/figures` |
| `figures.ocr.models-dir` | no | where the models are, e.g. `/app/models/ppocr` |
| `spring.jpa.show-sql`, `spring.thymeleaf.cache` | no | `false` / `true` in production |
| `server.port` | no | 8081 (or behind the proxy) |

## 5. The OCR models

`sh tools/ocr/fetch_models.sh /app/models/ppocr` (in the Dockerfile above)
downloads the two PP-OCRv6 model files (31 MB, Apache-2.0) from RapidOCR's
official host, checks their SHA-256, and adds the character list that is
kept in git (`tools/ocr/PP-OCRv6_rec_small.keys.txt`). Without the models
the app still runs; figure text falls back to Tesseract (worse).

## 6. CI pipeline (suggested)

1. `./mvnw -B verify` — 49 tests. `SmartGazetteApplicationTests` starts the
   whole Spring context and **needs a PostgreSQL** (a service container in
   GitHub Actions) and the settings above as environment variables (any
   dummy API keys work for the test).
2. `cd tools && npm ci && node ruled_test.js` — extractor regression cases
   (prints `ok`/`ERROR` lines).
3. Build the image (§3), push, deploy `develop` → staging, `main` →
   production.

Python is **not** needed in production; `tools/*.py` are the measurement lab.

## 7. What comes from where

| what | where it comes from | who does it |
|---|---|---|
| the app code, templates, schemas, law and county reference data, the extractor scripts (`tools/`) | the GitHub repository | CI clones it |
| Java libraries (Spring, ONNX Runtime, PDFBox...) | Maven Central, during `./mvnw package` | automatic in CI |
| Node package `@firecrawl/pdf-inspector` | npm, `npm ci` in `tools/` (versions pinned by `package-lock.json`) | automatic in the image build |
| poppler, Tesseract, Node.js, Java runtime | the operating system's packages (`apt-get` in the Dockerfile) | automatic in the image build |
| OCR models | RapidOCR's host, `tools/ocr/fetch_models.sh` (checksums in the script) | automatic in the image build |
| PostgreSQL | a managed database or a container | set up once by the CI/CD engineer |
| API keys (Groq, Gemini), database password | the owner's accounts | **the owner** types them into GitHub (Settings → Secrets and variables → Actions) or the host's environment settings; CI passes them to the app as environment variables (`GROQ_API_KEY`, `GEMINI_API_KEY`, `SPRING_DATASOURCE_PASSWORD`). Nobody else needs to see them. |

## 8. After deploying, check

- the home page loads; an admin upload of a small gazette finishes (logs:
  `Successfully finished processing`, `Saved N figures`);
- the log line `Extraction engine: pdf-inspector + cleaner` (not "legacy");
- a notice page shows its Tables / Figures / Laws Cited tabs.
