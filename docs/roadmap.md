# Roadmap

Future plans and feature requests for data-sync.

## Released Features

### Version 0.1.0 (Current)

- [x] YAML configuration support
- [x] Column mapping and renaming
- [x] Selective column syncing
- [x] Idempotent upsert operations
- [x] Compound primary keys
- [x] Database indexes with sort orders
- [x] Automatic index suggestions
- [x] Filename-based value extraction (template and regex)
- [x] Automatic stale record cleanup
- [x] Compound delete keys
- [x] Dry-run mode
- [x] PostgreSQL support
- [x] SQLite support
- [x] CLI interface with Click
- [x] Rich terminal output
- [x] Integration tests with real PostgreSQL
- [x] Type hints and MyPy support
- [x] Comprehensive documentation
- [x] GitHub Pages documentation site

## Planned Features

### Version 0.2.0 (Next Release)

Priority: **High**

- [ ] **CDF Science File Support**
  - Parse CDF (Common Data Format) files
  - Extract metadata from CDF headers
  - Support multi-dimensional variables
  - Handle CDF epochs and time conversions

- [ ] **Batch Processing**
  - Process multiple files in one command
  - Parallel processing for large batches
  - Progress bars for batch operations
  - Summary report after batch completion

- [ ] **Data Validation**
  - Schema validation before sync
  - Data type validation
  - Custom validation rules in config
  - Validation error reporting

### Version 0.3.0

Priority: **Medium**

- [ ] **Data Transformation**
  - Column value transformations (upper, lower, trim, etc.)
  - Date format conversions
  - Custom Python transformation functions
  - Expression-based transformations

- [ ] **Transaction Management**
  - Explicit transaction control
  - Rollback on errors
  - Savepoints for partial commits
  - Transaction logging

- [ ] **Schema Migration**
  - Track schema version
  - Automatic migrations
  - Migration rollback support
  - Migration history tracking

### Version 0.4.0

Priority: **Medium**

- [ ] **Additional Database Support**
  - MySQL/MariaDB support
  - Microsoft SQL Server support
  - Oracle database support
  - Database-agnostic abstraction layer

- [ ] **Advanced Logging**
  - Structured logging (JSON)
  - Log levels (DEBUG, INFO, WARN, ERROR)
  - Log file rotation
  - Integration with logging services

- [ ] **Performance Optimizations**
  - Bulk insert optimizations
  - Connection pooling
  - Streaming for large files
  - Memory-efficient processing

### Future Versions

Priority: **Low to Medium**

- [ ] **Web UI**
  - Configuration editor
  - Job monitoring dashboard
  - Sync history viewer
  - Real-time progress tracking

- [ ] **Scheduling**
  - Built-in job scheduler
  - Cron-like scheduling
  - Dependency management
  - Retry policies

- [ ] **Cloud Storage Support**
  - S3 file sources
  - Google Cloud Storage
  - Azure Blob Storage
  - Automatic file discovery

- [ ] **Notifications**
  - Email notifications on completion/failure
  - Slack/Discord webhooks
  - Custom notification handlers
  - Notification templates

- [ ] **Data Quality Checks**
  - Duplicate detection
  - Outlier detection
  - Completeness checks
  - Custom quality rules

- [ ] **Export Features**
  - Export database to CSV
  - Export to JSON
  - Export to Parquet
  - Incremental exports

## Community Requests

Features requested by the community (not yet planned):

- **Delta Lake support** - Sync to Delta Lake tables
- **Parquet file support** - Read from Parquet files
- **Excel file support** - Read from XLSX files
- **API mode** - Run as a web service
- **Real-time sync** - Watch files and sync on change
- **Encryption** - Encrypt data in transit and at rest

Want to suggest a feature? [Open an issue](https://github.com/yourusername/data-sync/issues/new)!

## Contributing

Want to help build these features? See the [Contributing Guide](contributing.md).

Priority areas for contributions:

1. **CDF file support** - Most requested feature
2. **Batch processing** - High impact for users
3. **Data validation** - Important for production use
4. **MySQL support** - Expand database compatibility

## Version History

### 0.1.0 (2024-01-15)

Initial release with core features:

- CSV to PostgreSQL/SQLite syncing
- Configuration-based jobs
- Column mapping
- Filename value extraction
- Automatic cleanup
- Dry-run mode
- CLI interface
- Comprehensive tests
- Full documentation

## Release Schedule

We aim for:

- **Major versions** (0.x.0): Every 3-4 months
- **Minor versions** (0.x.y): As needed for bug fixes
- **Patch versions** (0.x.y): Within days for critical bugs

## Backward Compatibility

We follow semantic versioning:

- **Major version** (1.0.0): Breaking changes allowed
- **Minor version** (0.x.0): New features, no breaking changes
- **Patch version** (0.0.x): Bug fixes only

Breaking changes will be:

1. Clearly documented
2. Announced in advance
3. Provided with migration guide
4. Minimized as much as possible

## Long-Term Vision

Our vision for data-sync:

### Year 1 (2024)

- Establish as reliable CSV/CDF sync tool
- Build active community
- Achieve >1000 GitHub stars
- Support PostgreSQL, MySQL, SQLite

### Year 2 (2025)

- Add batch processing and transformations
- Expand to 5+ database types
- Add web UI for configuration
- Achieve >5000 GitHub stars

### Year 3 (2026)

- Become the go-to tool for scientific data syncing
- Support real-time sync
- Add cloud storage integration
- Achieve >10000 GitHub stars

## Feedback

Your feedback shapes our roadmap! Please:

- ⭐ Star the repo if you find it useful
- 🐛 Report bugs
- 💡 Suggest features
- 💬 Join discussions
- 🤝 Contribute code

## Stay Updated

- **GitHub Releases**: Watch the repo for release notifications
- **Changelog**: See [CHANGELOG.md](https://github.com/yourusername/data-sync/blob/main/CHANGELOG.md)
- **Documentation**: This page is updated as plans change

Last updated: 2024-01-15
