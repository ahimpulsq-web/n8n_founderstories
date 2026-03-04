# N8N-FounderStories

A production-ready FastAPI backend service for automated lead generation, enrichment, and email campaign management. This system integrates with Google Maps, Hunter.io, web scraping, LLM-based content generation, and Google Sheets for comprehensive B2B outreach automation.

## 🚀 Features

### Core Capabilities
- **Multi-Source Lead Generation**: Google Maps Places API and Hunter.io integration
- **Intelligent Web Scraping**: Crawl4AI-powered content extraction with Playwright
- **LLM-Powered Enrichment**: Automated company analysis and email content generation
- **Email Campaign Management**: Personalized email generation with tracking
- **Google Sheets Integration**: Real-time data synchronization and export
- **PostgreSQL Database**: Robust data persistence with optimized batch operations
- **Background Workers**: Asynchronous processing for crawling, extraction, and updates

### Advanced Features
- **Search Plan Generation**: AI-powered query planning for targeted lead discovery
- **Industry Matching**: Semantic matching with 1,400+ industry categories
- **Email Validation**: Multi-stage validation with domain eligibility filtering
- **Rate Limiting**: Intelligent rate limiting for external APIs
- **Deadlock Prevention**: Robust database operations with retry logic
- **Comprehensive Logging**: Structured logging with request tracing

## 📋 Prerequisites

- **Python**: 3.10 or higher
- **PostgreSQL**: 12 or higher
- **Google Cloud**: Service account with Sheets and Drive API access
- **API Keys**: OpenRouter, Hunter.io, Google Maps, SerpAPI (optional)

## 🛠️ Installation

### 1. Clone Repository
```bash
git clone <repository-url>
cd N8N-FounderStories
```

### 2. Create Virtual Environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Download Spacy Model
```bash
python -m spacy download en_core_web_sm
```

### 5. Install Playwright Browsers
```bash
playwright install chromium
```

## ⚙️ Configuration

### 1. Environment Variables
Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
```

### 2. Required Configuration

#### Application Settings
```env
APP_NAME=N8N-FounderStories
ENVIRONMENT=production
HOST=0.0.0.0
PORT=8000
LOG_LEVEL=INFO
```

#### Database Configuration
```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=n8n_founderstories
POSTGRES_USERNAME=postgres
POSTGRES_PASSWORD=your-secure-password
ENABLE_POSTGRES=true
```

#### LLM Provider (OpenRouter)
```env
LLM_API_KEYS=your-openrouter-api-key
LLM_PREMIUM_MODELS=openai/gpt-4o-mini,google/gemini-2.5-flash
LLM_FREE_MODELS=meta-llama/llama-3.2-3b-instruct:free
```

#### External APIs
```env
HUNTER_API_KEY=your-hunter-api-key
GOOGLE_MAPS_API_KEY=your-google-maps-api-key
SERPAPI_API_KEY=your-serpapi-api-key
```

#### Google Sheets Integration
```env
GOOGLE_SERVICE_ACCOUNT_FILE=./credentials/service-account.json
GLOBAL_MAIL_TRACKING_SHEET_ID=your-sheet-id
```

### 3. Google Service Account Setup

1. Create a service account in Google Cloud Console
2. Enable Google Sheets API and Google Drive API
3. Download the JSON credentials file
4. Place it in `./credentials/service-account.json`
5. Share your Google Sheets with the service account email

## 🚀 Running the Application

### Development Mode
```bash
python -m n8n_founderstories
```

### Production Mode with Uvicorn
```bash
uvicorn n8n_founderstories.main:app --host 0.0.0.0 --port 8000 --workers 4
```

### Using Docker (Recommended for Production)
```bash
docker build -t n8n-founderstories .
docker run -d -p 8000:8000 --env-file .env n8n-founderstories
```

## 📡 API Endpoints

### Health Check
```bash
GET /api/v1/health
```

### Search Plan Generation
```bash
POST /api/v1/search-plan
Content-Type: application/json

{
  "prompt": "Find AI startups in Munich",
  "request_id": "unique-request-id"
}
```

### Google Maps Lead Generation
```bash
POST /api/v1/google-maps/search
Content-Type: application/json

{
  "request_id": "unique-request-id",
  "spreadsheet_id": "your-sheet-id",
  "queries": ["AI companies Munich"],
  "location": "Munich, Germany"
}
```

### Hunter.io Email Discovery
```bash
POST /api/v1/hunter/domain-search
Content-Type: application/json

{
  "request_id": "unique-request-id",
  "spreadsheet_id": "your-sheet-id",
  "domains": ["example.com", "company.de"]
}
```

### Email Campaign Management
```bash
POST /api/v1/mailer/prepare
Content-Type: application/json

{
  "request_id": "unique-request-id",
  "spreadsheet_id": "your-sheet-id",
  "domains": ["example.com"]
}
```

### Mail Tracking
```bash
POST /api/v1/mail_tracker/track
Content-Type: application/json

{
  "request_id": "unique-request-id",
  "thread_id": "msg_abc123",
  "company": "Example Corp",
  "domain": "example.com",
  "email": "contact@example.com",
  "send_status": "SENT"
}
```

## 🏗️ Architecture

### System Components

```
┌─────────────────────────────────────────────────────────────┐
│                      FastAPI Application                     │
├─────────────────────────────────────────────────────────────┤
│  API Endpoints  │  Background Workers  │  Database Layer    │
├─────────────────┼─────────────────────┼────────────────────┤
│ • Search Plan   │ • Crawler Worker    │ • PostgreSQL       │
│ • Google Maps   │ • LLM Worker        │ • Connection Pool  │
│ • Hunter.io     │ • Aggregate Worker  │ • Batch Operations │
│ • Mailer        │ • Email Generator   │ • Deadlock Retry   │
│ • Mail Tracker  │ • Sheets Updater    │                    │
└─────────────────┴─────────────────────┴────────────────────┘
         ↓                  ↓                      ↓
┌─────────────────┐ ┌──────────────┐ ┌────────────────────┐
│  External APIs  │ │  Web Scraper │ │  Google Sheets     │
├─────────────────┤ ├──────────────┤ ├────────────────────┤
│ • OpenRouter    │ │ • Crawl4AI   │ │ • Live Updates     │
│ • Hunter.io     │ │ • Playwright │ │ • Batch Export     │
│ • Google Maps   │ │ • Trafilatura│ │ • Formatting       │
│ • SerpAPI       │ │              │ │                    │
└─────────────────┘ └──────────────┘ └────────────────────┘
```

### Background Workers

1. **Crawler Worker**: Scrapes websites using Crawl4AI and Playwright
2. **LLM Extraction Worker**: Extracts structured data from scraped content
3. **Aggregate Worker**: Combines and enriches data from multiple sources
4. **Email Generator Worker**: Creates personalized email content using LLM
5. **Sheets Updater Worker**: Synchronizes data to Google Sheets every 30s

### Data Flow

```
User Request → API Endpoint → Job Creation → Database Storage
                                    ↓
                            Background Workers
                                    ↓
                    ┌───────────────┴───────────────┐
                    ↓                               ↓
            Data Enrichment                  Google Sheets
            (Crawl + LLM)                      Export
                    ↓                               ↓
            Master Results Table          Live Dashboard Updates
```

## 📊 Database Schema

### Core Tables

- **`mstr_results`**: Master results with enriched company data
- **`crawl_results`**: Raw crawled content from websites
- **`llm_ext_results`**: LLM-extracted structured data
- **`det_ext_results`**: Deterministically extracted emails
- **`enrichment_results`**: Aggregated enrichment data
- **`mail_content`**: Generated email content
- **`mail_tracker`**: Email send tracking and replies
- **`jobs`**: Job status and progress tracking

## 🔧 Development

### Project Structure
```
src/n8n_founderstories/
├── api/v1/              # API endpoints
├── core/                # Core utilities and config
│   ├── config.py        # Settings management
│   ├── db.py            # Database connection
│   ├── errors.py        # Error handling
│   └── utils/           # Utility functions
├── services/            # Business logic
│   ├── enrichment/      # Web scraping and extraction
│   ├── jobs/            # Job management
│   ├── mailer/          # Email generation
│   ├── master/          # Master data aggregation
│   ├── openrouter/      # LLM client
│   ├── search_plan/     # Search planning
│   ├── sheets/          # Google Sheets integration
│   └── sources/         # External data sources
└── main.py              # Application entry point
```

### Running Tests
```bash
pytest
```

### Code Quality
```bash
# Format code
black src/

# Type checking
mypy src/

# Linting
ruff check src/
```

## 🐛 Troubleshooting

### Common Issues

#### Database Connection Errors
```bash
# Check PostgreSQL is running
pg_isready -h localhost -p 5432

# Verify credentials in .env
psql -h localhost -U postgres -d n8n_founderstories
```

#### Google Sheets Permission Errors
- Ensure service account email has edit access to sheets
- Verify `GOOGLE_SERVICE_ACCOUNT_FILE` path is correct
- Check API quotas in Google Cloud Console

#### Playwright Browser Issues
```bash
# Reinstall browsers
playwright install --force chromium
```

#### Rate Limiting
- Monitor API usage in respective dashboards
- Adjust concurrency settings in `.env`
- Implement exponential backoff for retries

## 📈 Performance Optimization

### Database
- Use connection pooling (configured in settings)
- Batch operations for bulk inserts
- Indexes on frequently queried columns

### Web Scraping
- Adjust `DOMAIN_CONCURRENCY` based on server capacity
- Use `HEADLESS=true` for better performance
- Configure `CRAWL_TIMEOUT_S` appropriately

### LLM Calls
- Use `LLM_FREE_MODELS` for non-critical tasks
- Implement caching for repeated queries
- Batch similar requests when possible

## 🔒 Security

- Never commit `.env` files with real credentials
- Use strong PostgreSQL passwords
- Rotate API keys regularly
- Implement rate limiting on public endpoints
- Use HTTPS in production
- Keep dependencies updated

## 📝 License

[Your License Here]

## 🤝 Contributing

[Contributing Guidelines]

## 📧 Support

For issues and questions:
- GitHub Issues: [Repository Issues]
- Email: [Support Email]
- Documentation: [Docs URL]

## 🙏 Acknowledgments

- FastAPI for the excellent web framework
- Crawl4AI for advanced web scraping
- OpenRouter for LLM API aggregation
- Google Cloud for Sheets integration