# BACKLOG
_Future features and deferred work. Update when: deferring tasks, planning phases, capturing ideas._

## Product Vision
_What are we building and why?_

[Define your product vision here]

## Future Enhancements

### Phase 1: Core Features
- [ ] User authentication
- [ ] Data persistence patterns
- [ ] Basic CRUD operations

### Phase 2: Improvements
- [ ] Better error handling
- [ ] Request logging
- [ ] Admin interface
- [x] Export functionality ✅ #completed:2025-10-03

### Phase 3: Scale (if needed)
- [ ] Redis caching
- [ ] Background jobs
- [ ] Multiple workers
- [ ] Monitoring

### Google Ads Automation - Scalability
- [ ] Process 1M ads in 1-3 days with chunking strategy
- [x] Implement progress tracking and resume capability ✅ #completed:2025-10-02
- [ ] Add distributed caching (Redis) for multi-worker processing
- [ ] Create horizontal scaling with worker queue (Celery/RQ)
- [x] Build monitoring dashboard for batch processing status ✅ #completed:2025-10-02
- [ ] Add pause/resume controls to frontend ✅ #completed:2025-10-02 (implemented ahead of schedule)

## Technical Debt
- [ ] Add input validation
- [ ] Implement logging
- [ ] Add tests
- [ ] API documentation
- [ ] Create utility to split large Excel files into processable chunks (10k-50k rows)
- [ ] Add comprehensive error handling for Google Ads API failures
- [x] Add error handling to Thema Ads frontend ✅ #completed:2025-10-02

## Ideas Parking Lot
_Capture ideas for future consideration_

- **DM Review tool — slide 3 (Werkvoorraad) refresh**: deferred from the 2026-05-28 session. Slide 3 of `DM review_NEW.pptx` shows content coverage (FAQ%, Kopteksten%, AI-titles%, etc.) + URL counts (e.g. "389,994 URLs (+33%)"). User said "for sheet 3 I need to provide some extra context", and we shipped slide 2 only. Excel feed tabs for slide 3 are likely `new_visits`, `ut`, `canon`, `red`, `t&d`, `open_facets`, `werkvoorraad`, possibly `top_3_10_*` — see preview output in 2026-05-28 LEARNINGS.
- **Bulk CSV validation endpoint**: Pre-validate large CSVs before job creation (check customer IDs exist, ad groups are valid) - could save time by catching errors before job execution
- **Automated secret scanning in pre-commit hooks**: Prevent accidental commits of secrets with local validation before push (e.g., detect-secrets, git-secrets, or custom regex patterns)
- **Improve 202 retry logic for Cloudflare queuing**: Consider exponential backoff for HTTP 202 responses (2s, 5s, 10s) instead of single 2s retry - may reduce failure rate during high-load periods
- **Adaptive delay based on 202 response rate**: Monitor HTTP 202 response rate in real-time and dynamically adjust scraping delay to stay below Cloudflare's threshold. Start at 0.2s, increase to 0.5s if 202 rate exceeds 10%, decrease back to 0.2s if 202 rate drops below 2%. Would provide automatic optimization between speed and rate limit avoidance.

---
_Last updated: 2025-10-10_
