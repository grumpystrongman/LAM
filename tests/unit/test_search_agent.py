import unittest
import tempfile
import json
from pathlib import Path
from typing import Any
from unittest.mock import patch

import lam.interface.search_agent as search_agent_mod
from lam.interface.search_agent import EmailActionItem, execute_instruction, preview_instruction, resume_pending_plan


class TestSearchAgent(unittest.TestCase):
    def test_resolve_navigation_target_reuses_session_tab(self) -> None:
        with patch("lam.interface.search_agent.SessionManager") as mock_mgr, patch("lam.interface.search_agent.webbrowser.open") as mock_open:
            inst = mock_mgr.return_value
            inst.find_reusable_authenticated_tab.return_value = "https://mail.google.com/mail/u/0/#inbox"
            out = search_agent_mod._resolve_navigation_target(
                target_url="https://mail.google.com/mail/u/0/#inbox",
                recent_actions=["open_tab:https://mail.google.com/mail/u/0/#inbox"],
            )
            self.assertTrue(out["reused"])
            self.assertFalse(out["opened"])
            mock_open.assert_not_called()

    def test_resolve_navigation_target_opens_when_no_reuse(self) -> None:
        with patch("lam.interface.search_agent.SessionManager") as mock_mgr, patch("lam.interface.search_agent.webbrowser.open") as mock_open:
            inst = mock_mgr.return_value
            inst.find_reusable_authenticated_tab.return_value = ""
            inst.find_reusable_url.return_value = ""
            out = search_agent_mod._resolve_navigation_target(
                target_url="https://example.com/path/item",
                recent_actions=[],
            )
            self.assertFalse(out["reused"])
            self.assertTrue(out["opened"])
            mock_open.assert_called_once()

    def test_resolve_navigation_target_reuses_exact_file_url(self) -> None:
        with patch("lam.interface.search_agent.SessionManager") as mock_mgr, patch("lam.interface.search_agent.webbrowser.open") as mock_open:
            inst = mock_mgr.return_value
            inst.find_reusable_authenticated_tab.return_value = ""
            inst.find_reusable_url.return_value = "file:///C:/temp/dashboard.html"
            out = search_agent_mod._resolve_navigation_target(
                target_url="file:///C:/temp/dashboard.html",
                recent_actions=["open_tab:file:///C:/temp/dashboard.html"],
            )
            self.assertTrue(out["reused"])
            self.assertFalse(out["opened"])
            mock_open.assert_not_called()

    def test_artifact_reuse_index_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            doc = base / "document.md"
            doc.write_text("# test", encoding="utf-8")
            with patch("lam.interface.search_agent._artifact_reuse_index_path", return_value=base / "artifact_reuse_index.json"):
                search_agent_mod._remember_artifacts_for_reuse(
                    kind="artifact_generation",
                    instruction="Create report",
                    artifacts={"document_md": str(doc.resolve())},
                )
                reused = search_agent_mod._find_reusable_artifacts(
                    kind="artifact_generation",
                    instruction="Create report",
                    required_keys=["document_md"],
                    max_age_hours=24,
                )
                self.assertIn("document_md", reused)

    @patch("lam.interface.search_agent._open_target_with_reuse")
    @patch("lam.interface.search_agent._find_reusable_artifacts")
    def test_run_artifact_generation_reuses_existing_outputs(self, mock_reuse, mock_open_target) -> None:
        with tempfile.TemporaryDirectory() as td:
            doc = Path(td) / "document.md"
            doc.write_text("# existing", encoding="utf-8")
            mock_reuse.return_value = {"document_md": str(doc.resolve())}
            mock_open_target.return_value = (doc.resolve().as_uri(), {"decision": {"score": 75, "reasons": ["shortest_path_reuse_existing_state"]}})
            out = search_agent_mod._run_artifact_generation(
                plan={"deliverables": ["document"], "objective": "Create report"},
                instruction="Create report",
            )
            self.assertTrue(out.get("ok"))
            self.assertTrue(out.get("summary", {}).get("reused_existing_outputs"))
            self.assertIn("document_md", out.get("artifacts", {}))

    @patch("lam.interface.search_agent._open_target_with_reuse")
    @patch("lam.interface.search_agent._find_reusable_artifacts")
    def test_run_artifact_generation_always_regenerate_ignores_reuse(self, mock_reuse, mock_open_target) -> None:
        with tempfile.TemporaryDirectory() as td:
            doc = Path(td) / "document.md"
            doc.write_text("# existing", encoding="utf-8")
            mock_reuse.return_value = {"document_md": str(doc.resolve())}
            mock_open_target.return_value = (doc.resolve().as_uri(), {"decision": {"score": 90, "reasons": ["ok"]}})
            out = search_agent_mod._run_artifact_generation(
                plan={"deliverables": ["document"], "objective": "Create report"},
                instruction="Create report",
                artifact_reuse_mode="always_regenerate",
            )
            self.assertTrue(out.get("ok"))
            self.assertFalse(out.get("summary", {}).get("reused_existing_outputs"))
            self.assertEqual(out.get("summary", {}).get("artifact_reuse_mode"), "always_regenerate")

    def test_world_route_step_gate_locks_email_domain(self) -> None:
        allow, reason, _cost = search_agent_mod._world_route_step_gate(
            strategy="generic_research",
            attempt=1,
            instruction="scan my inbox",
            context={"email_intent": True, "reusable_gmail_tab": ""},
        )
        self.assertFalse(allow)
        self.assertEqual(reason, "domain_lock_email")

    def test_job_intent_detects_job_board_comp_prompt_without_artifact_words(self) -> None:
        prompt = (
            "look on all the job boards for a VP or AVP of Data and AI or VP or AVP Data and Analytics "
            "with hybrid or remote options that make more than 250k with total compensation above 370K"
        )
        self.assertTrue(search_agent_mod._is_job_research_intent(prompt))

    def test_extract_regions_defaults_to_us_only(self) -> None:
        regions = search_agent_mod._extract_regions(
            "Find VP data and AI roles with remote options and high compensation."
        )
        self.assertEqual(regions, ["us"])

    @patch("lam.interface.search_agent._open_target_with_reuse")
    @patch("lam.interface.search_agent._write_job_artifacts")
    @patch("lam.interface.search_agent._search_web")
    @patch("lam.interface.search_agent._scrape_builtin_jobs")
    @patch("lam.interface.search_agent._scrape_linkedin_jobs")
    def test_job_market_research_enforces_vp_avp_comp_and_region_constraints(
        self,
        mock_linkedin,
        mock_builtin,
        mock_search_web,
        mock_write_artifacts,
        mock_open_target,
    ) -> None:
        mock_linkedin.return_value = [
            search_agent_mod.JobListing(
                title="AVP, Data and AI",
                url="https://jobs.example.com/us-avp",
                source="linkedin",
                location="Austin, TX",
                remote=True,
                salary_text="$260k - $390k total compensation $405k",
                salary_min=260000.0,
                salary_max=390000.0,
                currency="USD",
                snippet="Hybrid role with total compensation $405k.",
            ),
            search_agent_mod.JobListing(
                title="Director, Data and Analytics",
                url="https://jobs.example.com/us-director",
                source="linkedin",
                location="Chicago, IL",
                remote=True,
                salary_text="$300k - $420k",
                salary_min=300000.0,
                salary_max=420000.0,
                currency="USD",
                snippet="Leadership role",
            ),
            search_agent_mod.JobListing(
                title="VP Data and AI",
                url="https://jobs.example.com/ie-vp",
                source="linkedin",
                location="Dublin, Ireland",
                remote=True,
                salary_text="€280k - €410k",
                salary_min=280000.0,
                salary_max=410000.0,
                currency="EUR",
                snippet="Dublin based",
            ),
        ]
        mock_builtin.return_value = []
        mock_search_web.return_value = []
        mock_write_artifacts.return_value = {"dashboard_html": ""}
        mock_open_target.return_value = ("", {"reused": False, "opened": False})

        out = search_agent_mod._run_job_market_research(
            "look on all the job boards for a VP or AVP of Data and AI or VP or AVP Data and Analytics "
            "with hybrid or remote options that make more than 250k with total compensation above 370K"
        )
        self.assertTrue(out.get("ok"))
        rows = out.get("results", [])
        self.assertEqual(len(rows), 1)
        self.assertIn("avp", rows[0].get("title", "").lower())
        self.assertIn("tx", rows[0].get("location", "").lower())
        self.assertNotIn("ireland", json.dumps(rows).lower())

    def test_native_plan_includes_playbook_validation(self) -> None:
        plan = search_agent_mod._build_native_plan(
            "Scan my inbox (cmajeff@gmail.com) for emails from the last 48 hours and draft replies"
        )
        self.assertIn("playbook_validation", plan)
        self.assertIn("playbook_graph_validation", plan)
        self.assertIn("playbook_step_obligations", plan)
        self.assertTrue(plan.get("playbook_validation", {}).get("ok"))
        self.assertTrue(plan.get("playbook_graph_validation", {}).get("ok"))

    def test_execute_native_plan_blocks_invalid_playbook(self) -> None:
        bad_plan = {
            "domain": "email_triage",
            "objective": "bad",
            "playbook_validation": {"ok": False, "errors": ["bad-step"]},
            "steps": [{"kind": "web_search"}],
        }
        out = search_agent_mod._execute_native_plan(bad_plan, instruction="bad plan")
        self.assertFalse(out.get("ok"))
        self.assertEqual(out.get("error_code"), "playbook_validation_failed")

    def test_execute_native_plan_blocks_invalid_playbook_graph(self) -> None:
        bad_plan = {
            "domain": "email_triage",
            "objective": "bad graph",
            "playbook_validation": {"ok": True, "errors": []},
            "playbook_graph_validation": {"ok": False, "errors": ["bad-transition"]},
            "steps": [{"kind": "list_recent_messages"}, {"kind": "save_csv"}],
        }
        out = search_agent_mod._execute_native_plan(bad_plan, instruction="bad graph")
        self.assertFalse(out.get("ok"))
        self.assertEqual(out.get("error_code"), "playbook_graph_validation_failed")

    @patch("lam.interface.search_agent._run_reflective_planner")
    def test_execute_native_plan_blocks_failed_obligations(self, mock_run) -> None:
        mock_run.return_value = {
            "ok": True,
            "mode": "email_triage",
            "query": "newer_than:2d in:inbox",
            "results_count": 2,
            "results": [],
            "artifacts": {},
            "summary": {},
            "source_status": {"gmail_ui": "ok"},
            "opened_url": "",
            "paused_for_credentials": False,
            "pause_reason": "",
            "trace": [],
        }
        plan = search_agent_mod._build_native_plan(
            "Scan my inbox (cmajeff@gmail.com) for emails from the last 48 hours, identify anything requiring action, create a task list in a spreadsheet, and draft replies for each."
        )
        out = search_agent_mod._execute_native_plan(plan, instruction="inbox triage")
        self.assertFalse(out.get("ok"))
        self.assertEqual(out.get("error_code"), "playbook_obligation_failed")

    @patch("lam.interface.search_agent._fetch_text")
    def test_search_ebay_listings_parses_price_cards(self, mock_fetch_text) -> None:
        mock_fetch_text.return_value = """
        <html><body>
          <li class="s-item">
            <a class="s-item__link" href="https://www.ebay.com/itm/111">
              <div class="s-item__title">Cyberdeck Raspberry Pi Build A</div>
              <span class="s-item__price">$189.00</span>
              <div class="s-item__subtitle">Used condition</div>
            </a>
          </li>
          <li class="s-item">
            <a class="s-item__link" href="https://www.ebay.com/itm/222">
              <div class="s-item__title">Cyberdeck Raspberry Pi Build B</div>
              <span class="s-item__price">$149.99</span>
            </a>
          </li>
        </body></html>
        """
        rows = search_agent_mod._search_ebay_listings(
            query="cyberdeck raspberry pi",
            search_url="https://www.ebay.com/sch/i.html?_nkw=cyberdeck+raspberry+pi&_sop=12",
            limit=5,
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].source, "ebay")
        self.assertEqual(rows[0].url, "https://www.ebay.com/itm/111")
        self.assertAlmostEqual(float(rows[1].price or 0.0), 149.99, places=2)

    @patch("lam.interface.search_agent._search_ebay_listings_playwright")
    @patch("lam.interface.search_agent._fetch_text")
    def test_search_ebay_listings_uses_playwright_fallback_on_interstitial(
        self,
        mock_fetch_text,
        mock_playwright_rows,
    ) -> None:
        mock_fetch_text.return_value = "<html><title>Pardon Our Interruption...</title></html>"
        mock_playwright_rows.return_value = [
            search_agent_mod.SearchResult(
                title="Cyberdeck Raspberry Pi Build C",
                url="https://www.ebay.com/itm/333",
                price=199.0,
                source="ebay",
                snippet="Fallback row",
            )
        ]
        rows = search_agent_mod._search_ebay_listings(
            query="cyberdeck raspberry pi",
            search_url="https://www.ebay.com/sch/i.html?_nkw=cyberdeck+raspberry+pi&_sop=12",
            limit=5,
            browser_worker_mode="local",
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].url, "https://www.ebay.com/itm/333")
        mock_playwright_rows.assert_called_once()

    def test_clean_ebay_query_removes_best_price_prompt_wrapping(self) -> None:
        cleaned = search_agent_mod._clean_ebay_query(
            "search ebay for best price on cyberdeck raspberry pi and recommend me the one to buy"
        )
        self.assertEqual(cleaned, "cyberdeck raspberry pi")

    def test_pick_recommended_candidate_prefers_lowest_price_then_quality(self) -> None:
        picked = search_agent_mod._pick_recommended_candidate(
            [
                {"title": "A", "url": "https://example.com/a", "price": 299.0, "rating": 4.9, "review_count": 11},
                {"title": "B", "url": "https://example.com/b", "price": 249.0, "rating": 3.8, "review_count": 5},
                {"title": "C", "url": "https://example.com/c", "price": 249.0, "rating": 4.5, "review_count": 88},
            ]
        )
        self.assertEqual(picked.get("title"), "C")

    @patch("lam.interface.search_agent._resolve_navigation_target")
    @patch("lam.interface.search_agent._search_ebay_listings")
    @patch("lam.interface.search_agent._search_web")
    def test_execute_instruction_ebay_price_recommendation_selects_lowest_listing(
        self,
        mock_search_web,
        mock_search_ebay,
        mock_resolve_nav,
    ) -> None:
        mock_search_web.return_value = [
            search_agent_mod.SearchResult(
                title="Cyberdeck Raspberry Pi for sale | eBay",
                url="https://www.ebay.com/sch/i.html?_nkw=cyberdeck+raspberry+pi&_sop=12",
                price=None,
                source="duckduckgo",
                snippet="Marketplace landing page.",
            )
        ]
        mock_search_ebay.return_value = [
            search_agent_mod.SearchResult(
                title="Cyberdeck Raspberry Pi Build A",
                url="https://www.ebay.com/itm/111",
                price=189.0,
                source="ebay",
                snippet="Used",
            ),
            search_agent_mod.SearchResult(
                title="Cyberdeck Raspberry Pi Build B",
                url="https://www.ebay.com/itm/222",
                price=149.99,
                source="ebay",
                snippet="Like new",
            ),
        ]
        mock_resolve_nav.return_value = {
            "url": "https://www.ebay.com/itm/222",
            "reused": False,
            "opened": True,
            "decision": {"score": 92.0, "reasons": ["ok"], "elegance_cost": 0},
        }
        out = execute_instruction(
            "search ebay for cyberdeck raspberry pi best price and recommend me the one to buy",
            control_granted=True,
            progress_cb=None,
        )
        self.assertTrue(out.get("ok"))
        self.assertEqual(out.get("mode"), "web_search")
        self.assertEqual(out.get("opened_url"), "https://www.ebay.com/itm/222")
        self.assertEqual(out.get("best_result", {}).get("url"), "https://www.ebay.com/itm/222")
        self.assertEqual(out.get("recommendation", {}).get("selected_url"), "https://www.ebay.com/itm/222")
        self.assertAlmostEqual(float(out.get("recommendation", {}).get("selected_price") or 0.0), 149.99, places=2)
        self.assertEqual(out.get("canvas", {}).get("title"), "Best Price Recommendation")
        self.assertIn("decision_matrix_csv", out.get("artifacts", {}))
        self.assertIn("recommendation_md", out.get("artifacts", {}))
        self.assertGreaterEqual(int(out.get("summary", {}).get("marketplace_candidate_count", 0) or 0), 1)

    def test_strategy_order_prefers_email_when_reusable_session_exists(self) -> None:
        with patch("lam.interface.search_agent.SessionManager") as mock_mgr:
            inst = mock_mgr.return_value
            inst.find_reusable_authenticated_tab.return_value = "https://mail.google.com/mail/u/0/#inbox"
            order = search_agent_mod._strategy_order(preferred="web_research", instruction="scan inbox and draft replies")
            self.assertGreaterEqual(len(order), 1)
            self.assertEqual(order[0], "email_triage")

    @patch("lam.interface.search_agent._search_web")
    def test_generic_research_superlative_requires_comparison_candidates(self, mock_search) -> None:
        mock_search.return_value = [
            search_agent_mod.SearchResult(
                title="Whiskey category page",
                url="https://example.com/search?q=whiskey",
                price=None,
                source="duckduckgo",
                snippet="Generic category listing.",
            )
        ]
        out = search_agent_mod._run_generic_research(
            "Find the best whiskey and tell me which one to buy.",
            progress_cb=None,
        )
        self.assertFalse(out.get("ok"))
        self.assertEqual(out.get("summary", {}).get("error"), "decision_quality_insufficient")

    @patch("lam.interface.search_agent._search_web")
    def test_generic_research_locality_gate_blocks_non_local_results(self, mock_search) -> None:
        mock_search.return_value = [
            search_agent_mod.SearchResult(
                title="Premium whiskey global catalog",
                url="https://catalog.example.com/whiskey/expensive",
                price=399.0,
                source="duckduckgo",
                snippet="Top bottles worldwide.",
            ),
            search_agent_mod.SearchResult(
                title="Whiskey buyer guide",
                url="https://reviews.example.com/product/whiskey-guide",
                price=None,
                source="duckduckgo",
                snippet="How to choose premium bourbon.",
            ),
        ]
        out = search_agent_mod._run_generic_research(
            "Find the most expensive whiskey locally in Durham, NC.",
            progress_cb=None,
        )
        self.assertFalse(out.get("ok"))
        self.assertEqual(out.get("summary", {}).get("error"), "locality_not_satisfied")

    @patch("lam.interface.search_agent._search_web")
    @patch("lam.interface.search_agent._write_generic_research_artifacts")
    @patch("lam.interface.search_agent.webbrowser.open")
    @patch("lam.interface.search_agent._relevance_score", return_value=2.0)
    def test_generic_research_includes_judgment_summary(self, _mock_rel, _mock_open, mock_write, mock_search) -> None:
        mock_search.return_value = [
            search_agent_mod.SearchResult(
                title="Concrete product listing in Durham",
                url="https://store.example.com/product/whiskey/durham-nc",
                price=199.0,
                source="duckduckgo",
                snippet="Durham NC inventory available.",
            ),
            search_agent_mod.SearchResult(
                title="Another product listing in Durham",
                url="https://shop.example.com/item/rare-whiskey-durham",
                price=249.0,
                source="duckduckgo",
                snippet="Local price and pickup in Durham.",
            ),
        ]
        with tempfile.TemporaryDirectory() as td:
            out_dir = Path(td)
            html_path = out_dir / "dashboard.html"
            html_path.write_text("<html></html>", encoding="utf-8")
            mock_write.return_value = {"dashboard_html": str(html_path), "primary_open_file": str(html_path)}
            out = search_agent_mod._run_generic_research(
                "Research whiskey options and produce a report.",
                progress_cb=None,
            )
        self.assertTrue(out.get("ok"))
        self.assertIn("judgment", out.get("summary", {}))

    @patch("lam.interface.search_agent.webbrowser.open")
    def test_focus_auth_session_does_not_reopen_existing_tab_by_default(self, mock_open) -> None:
        with patch.dict(
            "lam.interface.search_agent._EMAIL_AUTH_SESSIONS",
            {
                "sid-existing": {
                    "url": "https://mail.google.com/",
                    "opened_once": True,
                    "created_ts": 100.0,
                }
            },
            clear=True,
        ):
            r1 = search_agent_mod.focus_auth_session("sid-existing", allow_reopen=False)
            self.assertTrue(r1.get("ok"))
            self.assertTrue(r1.get("already_open"))
            mock_open.assert_not_called()

            r2 = search_agent_mod.focus_auth_session("sid-existing", allow_reopen=True)
            self.assertTrue(r2.get("ok"))
            self.assertEqual(mock_open.call_count, 1)

    @patch("lam.interface.search_agent.webbrowser.open")
    def test_focus_auth_session_sanitizes_bad_fallback_url(self, mock_open) -> None:
        with patch.dict("lam.interface.search_agent._EMAIL_AUTH_SESSIONS", {}, clear=True):
            r = search_agent_mod.focus_auth_session(
                auth_session_id="missing",
                fallback_url="https://myaccount.google.com/find-your-phone",
                allow_reopen=True,
            )
            self.assertFalse(r.get("ok"))
            self.assertEqual(r.get("error"), "auth_session_not_found")
            self.assertEqual(r.get("opened_url"), "https://mail.google.com/")
            mock_open.assert_called_once()
            opened_arg = str(mock_open.call_args.args[0])
            self.assertEqual(opened_arg, "https://mail.google.com/")

    @patch("lam.interface.search_agent.webbrowser.open")
    def test_focus_auth_session_uses_latest_when_id_missing_and_sanitizes_target(self, mock_open) -> None:
        with patch.dict(
            "lam.interface.search_agent._EMAIL_AUTH_SESSIONS",
            {
                "sid-a": {
                    "created_ts": 10.0,
                    "url": "https://myaccount.google.com/find-your-phone",
                    "opened_once": False,
                }
            },
            clear=True,
        ):
            r = search_agent_mod.focus_auth_session(
                auth_session_id="",
                fallback_url="https://mail.google.com/",
                allow_reopen=True,
            )
            self.assertTrue(r.get("ok"))
            self.assertEqual(r.get("auth_session_id"), "sid-a")
            self.assertEqual(r.get("opened_url"), "https://mail.google.com/")
            mock_open.assert_called_once_with("https://mail.google.com/", new=2)

    def test_select_latest_auth_session_prefers_recent(self) -> None:
        with patch.dict(
            "lam.interface.search_agent._EMAIL_AUTH_SESSIONS",
            {
                "sid-old": {"created_ts": 10.0, "url": "https://mail.google.com/"},
                "sid-new": {"created_ts": 20.0, "url": "https://mail.google.com/"},
            },
            clear=True,
        ):
            sid, sess = search_agent_mod._select_latest_auth_session("")
            self.assertEqual(sid, "sid-new")
            self.assertEqual(sess.get("url"), "https://mail.google.com/")

    @patch("lam.interface.search_agent.focus_auth_session")
    @patch("lam.interface.search_agent._attach_or_launch_auth_context")
    @patch("playwright.sync_api.sync_playwright")
    def test_email_triage_pauses_when_auth_context_locked(self, mock_sync_playwright, mock_attach_ctx, mock_focus_auth) -> None:
        class _FakePlaywright:
            class chromium:  # type: ignore[override]
                pass

            def stop(self) -> None:
                return None

        class _Factory:
            def start(self) -> _FakePlaywright:
                return _FakePlaywright()

        mock_sync_playwright.return_value = _Factory()
        mock_attach_ctx.return_value = None
        mock_focus_auth.return_value = {"ok": True, "opened_url": "https://mail.google.com/", "already_open": True}
        with patch.dict(
            "lam.interface.search_agent._EMAIL_AUTH_SESSIONS",
            {"sid-lock": {"created_ts": 1.0, "url": "https://mail.google.com/", "opened_once": True}},
            clear=True,
        ):
            out = search_agent_mod._run_email_triage_active_browser(
                instruction="Scan my inbox (cmajeff@gmail.com) for emails from the last 48 hours and draft replies.",
                manual_auth_phase=False,
                auth_session_id="sid-lock",
            )
            self.assertFalse(out.get("ok"))
            self.assertTrue(out.get("paused_for_credentials"))
            self.assertEqual(out.get("error_code"), "credential_missing")
            self.assertEqual(out.get("source_status", {}).get("gmail_ui"), "auth_profile_locked")

    @patch("lam.interface.search_agent.ensure_browser_worker")
    def test_email_triage_docker_mode_reports_worker_unavailable(self, mock_ensure_worker) -> None:
        mock_ensure_worker.return_value = {"ok": False, "detail": "docker not running"}
        out = search_agent_mod._run_email_triage_active_browser(
            instruction="Scan my inbox for last 48 hours and draft replies.",
            manual_auth_phase=False,
            auth_session_id="",
            browser_worker_mode="docker",
        )
        self.assertFalse(out.get("ok"))
        self.assertEqual(out.get("error_code"), "browser_worker_unavailable")
        self.assertFalse(bool(out.get("paused_for_credentials", False)))

    @patch("lam.interface.search_agent._launch_persistent_chrome_with_retry")
    def test_attach_or_launch_auth_context_skips_automated_fallback_by_default(self, mock_launch) -> None:
        class _Chromium:
            def connect_over_cdp(self, _url: str, timeout: int = 0) -> Any:  # type: ignore[name-defined]
                _ = timeout
                raise RuntimeError("cdp unavailable")

        class _Playwright:
            chromium = _Chromium()

        out = search_agent_mod._attach_or_launch_auth_context(
            playwright=_Playwright(),
            profile_dir=Path("."),
            session={"debug_port": 9222},
        )
        self.assertIsNone(out)
        mock_launch.assert_not_called()

    @patch("lam.interface.search_agent.imaplib.IMAP4_SSL", side_effect=RuntimeError("Application-specific password required"))
    @patch("lam.interface.search_agent._resolve_gmail_vault_credentials")
    def test_imap_fallback_classifies_app_password_required(self, mock_creds, _mock_imap) -> None:
        mock_creds.return_value = {
            "ok": True,
            "username": "cmajeff@gmail.com",
            "password": "secret",
            "entry_id": "entry-1",
        }
        out = search_agent_mod._run_email_triage_imap_fallback(
            instruction="Scan my inbox for last 48 hours and draft replies.",
            account="cmajeff@gmail.com",
            progress_cb=None,
            trace=[],
        )
        self.assertFalse(out.get("ok"))
        self.assertEqual(out.get("error_code"), "imap_app_password_required")
        self.assertIn("app password", str(out.get("pause_reason", "")).lower())
        self.assertEqual(out.get("summary", {}).get("imap_error_code"), "imap_app_password_required")

    def test_control_gate(self) -> None:
        result = execute_instruction("search amazon for abu garcia voltiq baitcasting reel", control_granted=False)
        self.assertFalse(result["ok"])
        self.assertIn("Control not granted", result["error"])

    @patch("lam.interface.search_agent._search_web")
    @patch("lam.interface.search_agent.webbrowser.open")
    def test_search_flow_carries_freshness_metadata(self, _mock_open, mock_search) -> None:
        mock_search.return_value = [
            search_agent_mod.SearchResult(
                title="Example Item",
                url="https://example.com/item/1",
                price=None,
                source="duckduckgo",
                snippet="example",
            )
        ]
        result = execute_instruction(
            "find example item",
            control_granted=True,
            artifact_reuse_mode="always_regenerate",
            artifact_reuse_max_age_hours=12,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result.get("summary", {}).get("artifact_reuse_mode"), "always_regenerate")
        self.assertEqual(result.get("summary", {}).get("artifact_reuse_max_age_hours"), 12)

    @patch("lam.interface.search_agent.get_guidance")
    @patch("lam.interface.search_agent.execute_plan")
    def test_open_app_flow(self, mock_exec, mock_guidance) -> None:
        class R:
            ok = True
            trace = [{"step": 0, "action": "open_app", "ok": True}]
            done = False
            next_step_index = 1
            paused_for_credentials = True
            pause_reason = "Login checkpoint"
            error = ""

        mock_exec.return_value = R()
        mock_guidance.return_value = {"app_name": "chatgpt", "guidance": []}
        result = execute_instruction("open chatgpt app", control_granted=True)
        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "desktop_sequence")
        self.assertTrue(result["paused_for_credentials"])
        self.assertIsNotNone(result["pending_plan"])
        self.assertIn("task_envelope", result)
        self.assertIn("plan_contract", result)
        self.assertIn("execution_trace", result)
        self.assertIn("verification_report", result)
        self.assertIn("final_report", result)
        self.assertIn("summary", result)
        self.assertIn("elegance_budget", result.get("summary", {}))
        self.assertTrue(mock_exec.call_args.kwargs.get("human_like_interaction"))

    @patch("lam.interface.search_agent.get_guidance")
    @patch("lam.interface.search_agent.execute_plan")
    def test_open_app_flow_passes_human_like_interaction(self, mock_exec, mock_guidance) -> None:
        class R:
            ok = True
            trace = [{"step": 0, "action": "open_app", "ok": True}]
            done = True
            next_step_index = 1
            paused_for_credentials = False
            pause_reason = ""
            error = ""

        mock_exec.return_value = R()
        mock_guidance.return_value = {"app_name": "chatgpt", "guidance": []}
        result = execute_instruction(
            "open chatgpt app",
            control_granted=True,
            human_like_interaction=True,
        )
        self.assertTrue(result["ok"])
        self.assertTrue(mock_exec.call_args.kwargs.get("human_like_interaction"))

    @patch("lam.interface.search_agent.execute_plan")
    def test_resume_pending_plan(self, mock_exec) -> None:
        class R:
            ok = True
            trace = [{"step": 1, "action": "click", "ok": True}]
            done = True
            next_step_index = 2
            paused_for_credentials = False
            pause_reason = ""
            error = ""

        mock_exec.return_value = R()
        result = resume_pending_plan(
            {
                "plan": {
                    "steps": [
                        {"action": "open_app", "app": "chatgpt"},
                        {"action": "click", "selector": {"value": "button:Run"}},
                    ]
                },
                "next_step_index": 1,
            },
            step_mode=False,
        )
        self.assertTrue(result["ok"])
        self.assertTrue(result["done"])

    def test_preview_instruction(self) -> None:
        result = preview_instruction("open chatgpt app then click submit")
        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "preview_desktop_sequence")
        self.assertIn("risk", result)
        self.assertIn("planned_steps", result)
        self.assertIn("undo_plan", result)

    def test_preview_native_plan(self) -> None:
        result = preview_instruction("Research top AI data leadership jobs in US and Ireland then build spreadsheet and dashboard")
        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "preview_native_plan")
        self.assertIn("plan", result)

    @patch("lam.interface.search_agent._execute_native_plan")
    def test_job_research_flow(self, mock_exec_native) -> None:
        mock_exec_native.return_value = {
            "ok": True,
            "mode": "autonomous_plan_execute",
            "query": "VP Data and AI",
            "results_count": 3,
            "artifacts": {"dashboard_html": "C:\\temp\\dash.html", "jobs_csv": "C:\\temp\\jobs.csv"},
            "summary": {"total": 3},
            "results": [{"title": "VP Data and AI", "url": "https://example.com"}],
            "opened_url": "file:///C:/temp/dash.html",
            "canvas": {"title": "Job Market Dashboard Generated", "subtitle": "3 listings", "cards": []},
        }
        result = execute_instruction(
            "Search Indeed and LinkedIn for VP of Data and AI roles and build spreadsheet and dashboard",
            control_granted=True,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "autonomous_plan_execute")
        self.assertEqual(result["results_count"], 3)
        self.assertIn("verification", result)
        self.assertIn("report", result)
        self.assertIn("undo_plan", result)
        self.assertIn("elegance_budget", result.get("summary", {}))

    @patch("lam.interface.search_agent._run_competitor_analysis")
    def test_competitor_analysis_flow(self, mock_run) -> None:
        mock_run.return_value = {
            "ok": True,
            "mode": "competitor_analysis",
            "query": "Epic Systems EHR competitors",
            "results_count": 12,
            "artifacts": {
                "executive_summary_md": "C:\\temp\\executive_summary.md",
                "powerpoint_pptx": "C:\\temp\\executive_summary.pptx",
                "dashboard_html": "C:\\temp\\dashboard.html",
            },
            "summary": {
                "target": "Epic Systems",
                "top_competitors": ["Oracle Health (Cerner)"],
                "live_non_curated_citations": 4,
                "required_live_non_curated_citations": 3,
            },
            "results": [{"title": "Oracle Health vs Epic", "url": "https://example.com"}],
            "opened_url": "file:///C:/temp/dashboard.html",
            "canvas": {"title": "Epic Systems Competitor Analysis Ready", "subtitle": "Top 5", "cards": []},
        }
        result = execute_instruction(
            "Research the top 5 competitors to Epic Systems, build a 2-page executive summary, create a PowerPoint, and save everything to a folder called Epic Competitor Analysis.",
            control_granted=True,
            min_live_non_curated_citations=3,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "autonomous_plan_execute")
        self.assertIn("executive_summary_md", result["artifacts"])
        self.assertIn("powerpoint_pptx", result["artifacts"])
        self.assertIn("verification", result)

    @patch("lam.interface.search_agent._run_generic_research")
    @patch("lam.interface.search_agent._run_competitor_analysis")
    def test_competitor_analysis_strict_citation_gate(self, mock_comp, mock_generic) -> None:
        mock_comp.return_value = {
            "ok": False,
            "mode": "competitor_analysis",
            "query": "Epic Systems EHR competitors",
            "results_count": 2,
            "results": [],
            "artifacts": {},
            "summary": {
                "error": "insufficient_live_non_curated_citations",
                "live_non_curated_citations": 0,
                "required_live_non_curated_citations": 3,
            },
            "source_status": {},
            "opened_url": "",
            "canvas": {"title": "Run Blocked", "subtitle": "Strict citation gate", "cards": []},
        }
        mock_generic.return_value = {
            "ok": True,
            "query": "fallback generic query",
            "results_count": 20,
            "results": [{"title": "x", "url": "https://example.com", "source": "duckduckgo"}],
            "artifacts": {"report_md": "C:\\temp\\report.md"},
            "summary": {"total": 20},
            "source_status": {},
            "opened_url": "file:///C:/temp/report.md",
            "canvas": {"title": "Generic Result", "subtitle": "fallback", "cards": []},
        }
        result = execute_instruction(
            "Research the top 5 competitors to Epic Systems and build executive summary and PowerPoint",
            control_granted=True,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["summary"].get("error"), "strict_competitor_validation_failed")

    def test_destructive_instruction_requires_confirmation(self) -> None:
        result = execute_instruction(
            "delete all files in downloads",
            control_granted=True,
            confirm_risky=False,
        )
        self.assertFalse(result["ok"])
        self.assertTrue(result.get("requires_confirmation", False))
        self.assertIn("planned_steps", result)
        self.assertIn("undo_plan", result)

    @patch("lam.interface.search_agent._run_email_triage")
    @unittest.skip("Runtime-dependent path; behavior covered by targeted planner/credential tests.")
    def test_email_triage_flow(self, mock_email) -> None:
        mock_email.return_value = {
            "ok": True,
            "mode": "email_triage",
            "query": "newer_than:2d in:inbox",
            "results_count": 2,
            "results": [],
            "artifacts": {"email_tasks_csv": "C:\\temp\\task_list.csv", "email_triage_html": "C:\\temp\\dashboard.html"},
            "summary": {"messages_processed": 2, "action_required": 1, "drafts_created": 1},
            "source_status": {"gmail_ui": "ok"},
            "opened_url": "file:///C:/temp/dashboard.html",
            "paused_for_credentials": False,
            "pause_reason": "",
            "trace": [
                {"step": 0, "action": "list_recent_messages", "ok": True, "result_count": 2},
                {"step": 1, "action": "read_message", "ok": True, "result_count": 2},
                {"step": 2, "action": "create_draft", "ok": True, "result_count": 1},
                {"step": 3, "action": "save_csv", "ok": True, "artifact": "C:\\temp\\task_list.csv"},
                {"step": 4, "action": "present", "ok": True, "opened_url": "file:///C:/temp/dashboard.html"},
            ],
            "canvas": {"title": "Inbox Triage Completed", "subtitle": "1 action-needed emails", "cards": []},
        }
        result = execute_instruction(
            "Scan my inbox (cmajeff@gmail.com) for emails from the last 48 hours, identify anything requiring action, create a task list in a spreadsheet, and draft replies for each.",
            control_granted=True,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "autonomous_plan_execute")
        self.assertEqual(result.get("plan", {}).get("domain"), "email_triage")
        self.assertEqual(result.get("plan_contract", {}).get("primary_domain"), "email")
        self.assertEqual(result.get("plan_contract", {}).get("validation_status"), "valid")
        mock_email.assert_called_once()

    @patch("lam.interface.search_agent.focus_auth_session")
    @patch("lam.interface.search_agent._start_email_auth_session")
    def test_email_triage_manual_auth_phase_pauses(self, mock_start_auth, mock_focus) -> None:
        mock_start_auth.return_value = {"ok": True, "auth_session_id": "sid-auth"}
        mock_focus.return_value = {"ok": True, "opened_url": "https://mail.google.com/"}
        result = execute_instruction(
            "Scan my inbox (cmajeff@gmail.com) for emails from the last 48 hours, identify anything requiring action, create a task list in a spreadsheet, and draft replies for each.",
            control_granted=True,
            manual_auth_phase=True,
        )
        self.assertFalse(result["ok"])
        self.assertTrue(result.get("paused_for_credentials", False))
        self.assertEqual(result.get("error_code"), "credential_missing")
        self.assertEqual(result.get("summary", {}).get("error"), "manual_auth_phase")
        opened = result.get("opened_url", "")
        self.assertTrue(("accounts.google.com" in opened) or ("mail.google.com" in opened))
        self.assertEqual(result.get("auth_session_id"), "sid-auth")
        self.assertGreaterEqual(mock_start_auth.call_count, 1)

    @patch("lam.interface.search_agent._run_generic_research")
    @patch("lam.interface.search_agent._run_email_triage")
    def test_email_triage_credential_pause_does_not_fallback(self, mock_email, mock_generic) -> None:
        mock_email.return_value = {
            "ok": False,
            "mode": "email_triage",
            "query": "newer_than:2d in:inbox",
            "results_count": 0,
            "results": [],
            "artifacts": {},
            "summary": {"error": "credential_missing", "account": "cmajeff@gmail.com"},
            "source_status": {"gmail_ui": "ok"},
            "opened_url": "https://mail.google.com/",
            "paused_for_credentials": True,
            "pause_reason": "Google login required.",
            "error": "credential_missing",
            "error_code": "credential_missing",
            "trace": [{"step": 0, "action": "list_recent_messages", "ok": True, "opened_url": "https://mail.google.com/"}],
            "canvas": {"title": "Paused For Login", "subtitle": "Sign in", "cards": []},
        }
        mock_generic.return_value = {"ok": True, "query": "bad fallback", "results_count": 10, "results": [], "artifacts": {}}
        result = execute_instruction(
            "Scan my inbox (cmajeff@gmail.com) for emails from the last 48 hours, identify anything requiring action, create a task list in a spreadsheet, and draft replies for each.",
            control_granted=True,
        )
        self.assertFalse(result["ok"])
        self.assertTrue(result.get("paused_for_credentials", False))
        self.assertEqual(result.get("error_code"), "credential_missing")
        self.assertEqual(result.get("query"), "newer_than:2d in:inbox")
        mock_generic.assert_not_called()

    @unittest.skip("Covered by focused auth/session tests; end-to-end variant is unstable in CI runtime.")
    @patch("lam.interface.search_agent.webbrowser.open")
    def test_email_triage_active_browser_manual_auth_and_resume(self, mock_open) -> None:
        instruction = (
            "Scan my inbox (cmajeff@gmail.com) for emails from the last 48 hours, identify anything requiring action, "
            "create a task list in a spreadsheet, and draft replies for each."
        )
        paused = {
            "ok": False,
            "mode": "email_triage",
            "query": "newer_than:2d in:inbox",
            "results_count": 0,
            "results": [],
            "artifacts": {},
            "summary": {"error": "manual_auth_phase"},
            "source_status": {"gmail_ui": "manual_auth_phase"},
            "opened_url": "https://mail.google.com/",
            "paused_for_credentials": True,
            "pause_reason": "Sign in then resume.",
            "error": "credential_missing",
            "error_code": "credential_missing",
            "auth_session_id": "sid1",
            "trace": [],
            "canvas": {"title": "Paused For Manual Auth", "subtitle": "Sign in", "cards": []},
        }
        resumed = {
            "ok": True,
            "mode": "email_triage",
            "query": "newer_than:2d in:inbox",
            "results_count": 2,
            "results": [
                {
                    "message_id": "m1",
                    "sender": "a@example.com",
                    "subject": "Action required",
                    "received_at": "now",
                    "snippet": "x",
                    "requires_action": True,
                    "reason": "matched",
                    "draft_created": True,
                }
            ],
            "artifacts": {"email_tasks_csv": "C:\\temp\\tasks.csv", "email_triage_html": "C:\\temp\\dash.html"},
            "summary": {"messages_processed": 2, "action_required": 1, "drafts_created": 1, "auth_session_reused": True},
            "source_status": {"gmail_ui": "ok"},
            "opened_url": "file:///C:/temp/dash.html",
            "paused_for_credentials": False,
            "pause_reason": "",
            "trace": [],
            "auth_session_id": "sid1",
            "canvas": {"title": "Inbox Triage Completed", "subtitle": "done", "cards": []},
        }
        with patch("lam.interface.search_agent._run_email_triage", side_effect=[paused, resumed]):
            first = execute_instruction(instruction, control_granted=True, manual_auth_phase=True)
            self.assertFalse(first["ok"])
            self.assertTrue(first.get("paused_for_credentials", False))
            self.assertEqual(first.get("auth_session_id"), "sid1")
            second = execute_instruction(instruction, control_granted=True, manual_auth_phase=False, auth_session_id="sid1")
            self.assertTrue(second["ok"])
            self.assertEqual(second.get("source_status", {}).get("gmail_ui"), "ok")
            self.assertIn("email_tasks_csv", second.get("artifacts", {}))
            self.assertGreaterEqual(mock_open.call_count, 0)

    @patch("lam.interface.search_agent.webbrowser.open")
    def test_email_triage_without_session_id_enters_manual_auth_phase(self, mock_open) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            out_dir = root / "email_triage"
            out_dir.mkdir(parents=True, exist_ok=True)
            csv_path = out_dir / "task_list.csv"
            md_path = out_dir / "summary.md"
            html_path = out_dir / "dashboard.html"
            csv_path.write_text("message_id,sender\n", encoding="utf-8")
            md_path.write_text("# Inbox Triage Summary\n", encoding="utf-8")
            html_path.write_text("<html></html>", encoding="utf-8")

            with patch(
                "lam.interface.search_agent._write_email_triage_artifacts",
                return_value={
                    "directory": str(out_dir),
                    "email_tasks_csv": str(csv_path),
                    "summary_md": str(md_path),
                    "email_triage_html": str(html_path),
                    "primary_open_file": str(html_path),
                },
            ), patch(
                "lam.interface.search_agent._start_email_auth_session",
                return_value={"ok": True, "auth_session_id": "sid2"},
            ), patch(
                "lam.interface.search_agent.focus_auth_session",
                return_value={"ok": True, "opened_url": "https://mail.google.com/"},
            ):
                result = execute_instruction(
                    "Scan my inbox (cmajeff@gmail.com) for emails from the last 48 hours, identify anything requiring action, create a task list in a spreadsheet, and draft replies for each.",
                    control_granted=True,
                    manual_auth_phase=False,
                    auth_session_id="",
                )
                self.assertFalse(result["ok"])
                self.assertTrue(result.get("paused_for_credentials", False))
                self.assertEqual(result.get("error_code"), "credential_missing")
                self.assertEqual(result.get("source_status", {}).get("gmail_ui"), "manual_auth_phase")
                self.assertEqual(result.get("auth_session_id"), "sid2")
                self.assertGreaterEqual(mock_open.call_count, 0)

    @patch("lam.interface.search_agent._run_email_triage_imap_fallback")
    @patch("lam.interface.search_agent._attach_or_launch_auth_context")
    @patch("playwright.sync_api.sync_playwright")
    @unittest.skip("Runtime-dependent browser context behavior; covered by focused unit tests.")
    def test_email_triage_workspace_block_uses_imap_fallback(self, mock_sync_playwright, mock_attach_ctx, mock_fallback) -> None:
        class FakePage:
            def __init__(self) -> None:
                self.url = "https://workspace.google.com/intl/en-US/gmail/"

            def goto(self, _url: str, timeout: int = 0) -> None:
                _ = timeout
                return None

        class FakeContext:
            def __init__(self) -> None:
                self.pages = [FakePage()]

            def new_page(self) -> FakePage:
                return FakePage()

            def close(self) -> None:
                return None

        class _FakePlaywright:
            class chromium:  # type: ignore[override]
                pass

            def stop(self) -> None:
                return None

        class _Factory:
            def start(self) -> _FakePlaywright:
                return _FakePlaywright()

        mock_sync_playwright.return_value = _Factory()
        mock_attach_ctx.return_value = FakeContext()
        mock_fallback.return_value = {
            "ok": True,
            "mode": "email_triage",
            "source_status": {"gmail_imap": "ok", "gmail_ui": "fallback_imap"},
            "artifacts": {"email_tasks_csv": "C:\\temp\\task.csv"},
        }
        with patch.dict(
            "lam.interface.search_agent._EMAIL_AUTH_SESSIONS",
            {"sid-fallback": {"created_ts": 1.0, "url": "https://mail.google.com/", "opened_once": True}},
            clear=True,
        ), patch("lam.interface.search_agent._gmail_wait_ready_state", return_value="unknown"):
            result = execute_instruction(
                "Scan my inbox (cmajeff@gmail.com) for emails from the last 48 hours, identify anything requiring action, create a task list in a spreadsheet, and draft replies for each.",
                control_granted=True,
                manual_auth_phase=False,
                auth_session_id="sid-fallback",
            )
            self.assertTrue(result.get("ok"))
            self.assertEqual(result.get("source_status", {}).get("gmail_imap"), "ok")
            self.assertGreaterEqual(mock_fallback.call_count, 1)

    @patch("lam.interface.search_agent._run_email_triage_imap_fallback")
    @patch("lam.interface.search_agent._attach_or_launch_auth_context", side_effect=RuntimeError("ctx failed"))
    @patch("playwright.sync_api.sync_playwright")
    @unittest.skip("Runtime-dependent browser context behavior; covered by focused unit tests.")
    def test_email_triage_browser_exception_uses_imap_fallback(self, mock_sync_playwright, _mock_attach, mock_fallback) -> None:
        class _FakePlaywright:
            class chromium:  # type: ignore[override]
                pass

            def stop(self) -> None:
                return None

        class _Factory:
            def start(self) -> _FakePlaywright:
                return _FakePlaywright()

        mock_sync_playwright.return_value = _Factory()
        mock_fallback.return_value = {
            "ok": True,
            "mode": "email_triage",
            "source_status": {"gmail_imap": "ok", "gmail_ui": "fallback_imap"},
            "artifacts": {"email_tasks_csv": "C:\\temp\\task.csv"},
        }
        with patch.dict(
            "lam.interface.search_agent._EMAIL_AUTH_SESSIONS",
            {"sid-exc": {"created_ts": 1.0, "url": "https://mail.google.com/", "opened_once": True}},
            clear=True,
        ):
            result = execute_instruction(
                "Scan my inbox (cmajeff@gmail.com) for emails from the last 48 hours, identify anything requiring action, create a task list in a spreadsheet, and draft replies for each.",
                control_granted=True,
                manual_auth_phase=False,
                auth_session_id="sid-exc",
            )
            self.assertTrue(result.get("ok"))
            self.assertEqual(result.get("source_status", {}).get("gmail_imap"), "ok")
            self.assertGreaterEqual(mock_fallback.call_count, 1)

    @patch("lam.interface.search_agent.get_guidance")
    @patch("lam.interface.search_agent.execute_plan")
    @patch("lam.interface.search_agent.assess_risk")
    @patch("lam.interface.search_agent.build_plan")
    def test_communication_social_routes_to_desktop_sequence(self, mock_build_plan, mock_risk, mock_exec, mock_guidance) -> None:
        class R:
            ok = True
            trace = [{"step": 0, "action": "type_text", "ok": True}]
            done = True
            next_step_index = 1
            paused_for_credentials = False
            pause_reason = ""
            error = ""

        mock_build_plan.return_value = {
            "app_name": "whatsapp",
            "steps": [
                {"action": "open_app", "app": "whatsapp"},
                {"action": "type_text", "text": "Confirmed for 5pm."},
                {"action": "click", "selector": {"value": "Send"}},
            ],
        }
        mock_risk.return_value = {"requires_confirmation": False, "risky_steps": []}
        mock_exec.return_value = R()
        mock_guidance.return_value = {"app_name": "whatsapp", "guidance": []}
        result = execute_instruction("Respond to the WhatsApp chat confirming I can join at 5pm.", control_granted=True)
        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "desktop_sequence")
        mock_build_plan.assert_called_once()

    @patch("lam.interface.search_agent._search_web")
    @patch("lam.interface.search_agent.webbrowser.open")
    def test_productivity_asks_route_to_artifact_generation(self, mock_open, mock_search) -> None:
        result = execute_instruction(
            "Write a launch document, build a PPT, and create visuals for the Q3 kickoff.",
            control_granted=True,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "artifact_generation")
        self.assertIn("document_md", result.get("artifacts", {}))
        self.assertIn("powerpoint_pptx", result.get("artifacts", {}))
        self.assertIn("visual_html", result.get("artifacts", {}))
        mock_search.assert_not_called()
        self.assertTrue(str(result.get("opened_url", "")).startswith("file:///"))

    @patch("lam.interface.search_agent._execute_native_plan")
    @patch("lam.interface.search_agent._build_native_plan")
    @patch("lam.interface.search_agent._classify_explicit_route")
    def test_fail_fast_when_requested_outputs_not_in_native_plan(self, mock_route, mock_build_native, mock_exec_native) -> None:
        mock_route.return_value = ""
        mock_build_native.return_value = {
            "planner": "native-v1",
            "domain": "web_research",
            "objective": "Research AI infra trends and build a dashboard and PowerPoint",
            "deliverables": ["report"],
            "sources": ["web_search"],
            "constraints": {"prefer_public_pages": True},
            "steps": [
                {"kind": "research", "name": "Collect sources", "target": {"query": "ai infra"}},
                {"kind": "produce", "name": "Generate report", "target": {"path": "data/reports"}},
            ],
        }
        result = execute_instruction(
            "Research AI infra trends and build a dashboard and PowerPoint",
            control_granted=True,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["mode"], "native_plan_invalid")
        self.assertIn("powerpoint", result.get("missing_outputs", []))
        mock_exec_native.assert_not_called()


if __name__ == "__main__":
    unittest.main()
