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

## Fridged / Parked Work
_Work that exists in the codebase but is intentionally NOT wired into production. Pick up later._

- **Koptekst prompt v2** (parked 2026-05-22). New prompt lives in `dm-tools/backend/gpt_service_v2.py` (`SYSTEM_MESSAGE_V2`, `generate_product_content_v2`). NOT wired into `backend/main.py` — production still uses v1 in `backend/gpt_service.py`. Only consumer today is the benchmark script `dm-tools/scripts/koptekst_v2_comparison.py` (pulls N random URLs from `pa.kopteksten_content`, regenerates with v2, writes side-by-side Excel + aggregate metrics + both prompts to Downloads). Latest n=20 run: 20 v1 valid, 19 v2 valid (1 zero-product URL), v2 hits comparison-authority claim ~98% vs v1 ~0%, relative links ~91% vs v1 ~6%, opening-cliché rate 0% vs v1 ~94%. **Variation fix shipped**: `COMPARISON_AUTHORITY_PHRASINGS` (12 templates, varied syntax positions + quantifiers — "alle/diverse/uiteenlopende/talloze/breed aanbod/meerdere") picked at random per call in `build_user_prompt_v2`, system prompt now defers to the per-call hint and explicitly bans the "Op Beslist vind je veel aanbieders van ..." cliché which had become the default. n=20 sample showed 9 distinct templates used across 19 kopteksten with 0 cliché hits. **To activate**: swap the v1 import in `backend/main.py` for `generate_product_content_v2`, or add an env/query-param toggle for gradual cutover. Latest benchmark: `/mnt/c/Users/JoepvanSchagen/Downloads/claude/koptekst_v1_vs_v2_n20_variation.xlsx`.

---
_Last updated: 2026-05-22_
