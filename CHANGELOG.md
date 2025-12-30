# Changelog

## [1.0.0] - 2025-12-30

### Major Features Added

#### Three-Way Sync
- Added Storyteller DB integration for three-way progress sync
- Sync between Audiobookshelf ↔ KOSync ↔ Storyteller
- Anti-regression safeguards prevent accidental backwards sync
- Configurable sync thresholds for fine-tuning

#### Book Linker Workflow
- New Book Linker interface for preparing books for Storyteller
- Automated readaloud file monitoring and cleanup
- Safety checks prevent interference with active processing
- Folder structure preservation for organized libraries

#### Web Interface Enhancements
- Flask web UI for managing mappings
- Real-time progress display for all three systems
- Batch matching for bulk operations
- Search-on-demand for faster page loads
- Complete cleanup on delete (mappings, state, transcripts)

#### Configuration Improvements
- Environment variable-based configuration
- Flexible path mapping for different workflows
- Optional feature flags (Storyteller, Booklore, Book Linker)
- Docker Compose examples for easy setup

### Technical Improvements
- Unified Docker container (sync daemon + web server)
- Startup script for multi-process management
- Enhanced error handling and logging
- Performance optimizations for large libraries

### Credits
Based on the excellent [abs-kosync-bridge](https://github.com/jLichti/abs-kosync-bridge) 
by jLichti, with significant enhancements for advanced workflows.
