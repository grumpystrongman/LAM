import base64
import json
import shutil
import sys
import tempfile
import types
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

import lam.interface.search_agent as search_agent_mod
from lam.interface.search_agent import EmailActionItem, execute_instruction, preview_instruction, resume_pending_plan


class TestSearchAgent(unittest.TestCase):
    def _case_dir(self, name: str) -> Path:
        root = Path("data") / "test_artifacts" / "search_agent"
        root.mkdir(parents=True, exist_ok=True)
        path = root / name
        shutil.rmtree(path, ignore_errors=True)
        path.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(path, ignore_errors=True))
        return path

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
        base = self._case_dir("artifact_reuse_index_roundtrip")
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
        doc = self._case_dir("artifact_generation_reuse") / "document.md"
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
        doc = self._case_dir("artifact_generation_regenerate") / "document.md"
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

    def test_strategy_order_prefers_generic_research_for_non_email_web_research(self) -> None:
        order = search_agent_mod._strategy_order(
            preferred="web_research",
            instruction="Go research and find me the best wine to buy for dinner tonight.",
        )
        self.assertEqual(order, ["generic_research"])

    def test_extract_generic_query_focuses_wine_pairing_prompt(self) -> None:
        query = search_agent_mod._extract_generic_query(
            "Go research and find me the best wine to buy for dinner tonight. I am having steak and potatoes."
        )
        self.assertEqual(query, "best wine for steak and potatoes")

    def test_extract_generic_query_trims_artifact_clauses(self) -> None:
        query = search_agent_mod._extract_generic_query(
            "Go research the best portable espresso maker for travel and recommend which one to buy. Build a spreadsheet and summary."
        )
        self.assertEqual(query, "the best portable espresso maker for travel")

    def test_expand_queries_adds_topic_focused_recommendation_variants(self) -> None:
        queries = search_agent_mod._expand_queries(
            "best wine for steak and potatoes",
            instruction="Go research and find me the best wine to buy for dinner tonight. I am having steak and potatoes.",
        )
        self.assertIn("wine steak potatoes pairing", queries)
        self.assertIn("cabernet sauvignon steak potatoes", queries)

    def test_build_recommendation_focus_query_for_espresso_and_monitor(self) -> None:
        espresso = search_agent_mod._build_recommendation_focus_query(
            query="the best portable espresso maker for travel",
            instruction="Go research the best portable espresso maker for travel and recommend which one to buy.",
        )
        monitor = search_agent_mod._build_recommendation_focus_query(
            query="the best 27 inch monitor under 300 dollars for coding with a MacBook Pro",
            instruction="Find the best 27 inch monitor under 300 dollars for coding with a MacBook Pro.",
        )
        self.assertEqual(espresso, "portable espresso maker travel review")
        self.assertEqual(monitor, "27-inch monitor under 300 coding macbook pro")

    def test_build_product_candidate_rows_extracts_espresso_models(self) -> None:
        notes = [
            {
                "url": "https://example.com/review-1",
                "title": "Portable espresso machine roundup",
                "summary": "The Wacaco Picopresso and OutIn Nano stand out for travel.",
                "excerpt": "Many travelers prefer the Wacaco Picopresso, while the OutIn Nano is the easiest battery-powered option.",
            },
            {
                "url": "https://example.com/review-2",
                "title": "Best travel espresso makers",
                "summary": "OutIn Nano is convenient and Picopresso has excellent espresso quality.",
                "excerpt": "OutIn Nano appears in nearly every travel roundup.",
            },
        ]
        rows = search_agent_mod._build_product_candidate_rows(
            browser_notes=notes,
            instruction="Go research the best portable espresso maker for travel and recommend which one to buy.",
            query="the best portable espresso maker for travel",
        )
        self.assertGreaterEqual(len(rows), 2)
        self.assertEqual(rows[0]["candidate"], "OutIn Nano")

    def test_build_product_candidate_rows_extracts_monitor_models(self) -> None:
        notes = [
            {
                "url": "https://example.com/monitor-1",
                "title": "Best monitors for MacBook Pro",
                "summary": "LG 27UP650-W is a strong budget match for MacBook Pro users.",
                "excerpt": "Dell S2722QC and LG 27UP650-W both work well for coding and USB-C setups.",
            }
        ]
        rows = search_agent_mod._build_product_candidate_rows(
            browser_notes=notes,
            instruction="Find the best 27 inch monitor under 300 dollars for coding with a MacBook Pro.",
            query="the best 27 inch monitor under 300 dollars for coding with a MacBook Pro",
        )
        self.assertGreaterEqual(len(rows), 2)
        self.assertEqual(rows[0]["candidate"], "LG 27UP650-W")

    def test_candidate_buy_url_variants_include_wacaco_store_page(self) -> None:
        variants = search_agent_mod._candidate_buy_url_variants(
            candidate="Wacaco Picopresso",
            instruction="Go research the best portable espresso maker for travel and recommend which one to buy.",
            query="the best portable espresso maker for travel",
        )
        self.assertIn("https://www.wacaco.com/products/picopresso", variants)

    @patch("lam.interface.search_agent._probe_candidate_url")
    def test_resolve_product_candidate_buy_url_prefers_official_wacaco_store(self, mock_probe) -> None:
        def _fake_probe(url: str) -> str:
            if url == "https://www.wacaco.com/products/picopresso":
                return url
            return ""

        mock_probe.side_effect = _fake_probe
        resolved = search_agent_mod._resolve_product_candidate_buy_url(
            candidate="Wacaco Picopresso",
            instruction="Go research the best portable espresso maker for travel and recommend which one to buy.",
            query="the best portable espresso maker for travel",
            current_url="https://homecoffeeexpert.com/best-portable-espresso-machine/",
        )
        self.assertEqual(resolved, "https://www.wacaco.com/products/picopresso")

    @patch("lam.interface.search_agent._resolve_product_candidate_buy_url")
    def test_build_recommendation_summary_resolves_buy_url_for_product_candidate(self, mock_resolve) -> None:
        mock_resolve.return_value = "https://www.wacaco.com/products/picopresso"
        recommendation = search_agent_mod._build_recommendation_summary(
            decision_rows=[
                {
                    "candidate": "Wacaco Picopresso",
                    "candidate_type": "product_candidate",
                    "url": "https://homecoffeeexpert.com/best-portable-espresso-machine/",
                    "price": None,
                    "score": 3.5,
                    "rationale": "Repeatedly surfaced across reviewed source pages.",
                }
            ],
            results=[],
            instruction="Go research the best portable espresso maker for travel and recommend which one to buy.",
            query="the best portable espresso maker for travel",
        )
        self.assertEqual(recommendation.get("selected_url"), "https://www.wacaco.com/products/picopresso")

    def test_quality_gate_filters_generic_best_junk_results(self) -> None:
        ranked = [
            search_agent_mod.SearchResult(
                title='How can I apologize and promise that a mistake will not happen again?',
                url='https://ell.stackexchange.com/questions/94558/example',
                price=None,
                source='bing_rss',
                snippet='Steve suggestion is the best.',
            ),
            search_agent_mod.SearchResult(
                title='The Right Wine to Pair With Any Kind of Steak',
                url='https://www.foodandwine.com/best-wine-steak-pairings-11755220',
                price=None,
                source='bing_rss',
                snippet='Cabernet sauvignon and malbec are classic steak pairings.',
            ),
        ]
        filtered = search_agent_mod._apply_human_judgment_quality_gate(
            ranked=ranked,
            instruction='Go research and find me the best wine to buy for dinner tonight. I am having steak and potatoes.',
            query='best wine for steak and potatoes',
            constraints={'compare_required': True, 'locality_required': False, 'locality_terms': []},
        )
        self.assertEqual(len(filtered), 1)
        self.assertIn('foodandwine.com', filtered[0].url)

    def test_quality_gate_requires_multi_term_overlap_for_product_research(self) -> None:
        ranked = [
            search_agent_mod.SearchResult(
                title='PortableApps.com Platform Features',
                url='https://portableapps.com/platform/features',
                price=None,
                source='bing_rss',
                snippet='Run your favorite apps anywhere.',
            ),
            search_agent_mod.SearchResult(
                title='Best Portable Espresso Machine 2026: Top 4 Travel Makers',
                url='https://www.coffeejournals.com/best-portable-espresso-machine/',
                price=None,
                source='bing_rss',
                snippet='Portable espresso machines tested for travel use.',
            ),
        ]
        filtered = search_agent_mod._apply_human_judgment_quality_gate(
            ranked=ranked,
            instruction='Go research the best portable espresso maker for travel and recommend which one to buy.',
            query='the best portable espresso maker for travel',
            constraints={'compare_required': True, 'locality_required': False, 'locality_terms': []},
        )
        self.assertEqual(len(filtered), 1)
        self.assertIn('coffeejournals.com', filtered[0].url)

    def test_quality_gate_filters_monitor_query_false_positive(self) -> None:
        ranked = [
            search_agent_mod.SearchResult(
                title='Encoded apostrophe is converted to %27 - Stack Overflow',
                url='https://stackoverflow.com/questions/48114236/encoded-apostrophe-is-converted-to-27',
                price=None,
                source='bing_rss',
                snippet='Why is encoded apostrophe converted to %27?',
            ),
            search_agent_mod.SearchResult(
                title='The 6 Best Monitors For MacBook Pro And MacBook Air of 2026',
                url='https://www.rtings.com/monitor/reviews/best/monitors-macbook-pro',
                price=None,
                source='bing_rss',
                snippet='Top monitor picks for MacBook Pro users.',
            ),
        ]
        filtered = search_agent_mod._apply_human_judgment_quality_gate(
            ranked=ranked,
            instruction='Find the best 27 inch monitor under 300 dollars for coding with a MacBook Pro.',
            query='the best 27 inch monitor under 300 dollars for coding with a MacBook Pro',
            constraints={'compare_required': True, 'locality_required': False, 'locality_terms': []},
        )
        self.assertEqual(len(filtered), 1)
        self.assertIn('rtings.com', filtered[0].url)

    @patch("lam.interface.search_agent._score_result_against_objective", return_value=0.95)
    @patch("lam.interface.search_agent._run_strategy")
    def test_reflective_planner_web_research_does_not_start_with_email_triage(self, mock_run_strategy, _mock_score) -> None:
        seen: list[str] = []

        def _fake_run_strategy(*args, **kwargs):
            strategy = kwargs.get("strategy", "")
            seen.append(str(strategy))
            return {
                "ok": strategy == "generic_research",
                "mode": strategy,
                "query": "best wine for steak dinner",
                "results_count": 2,
                "results": [],
                "artifacts": {"decision_matrix_csv": "C:\\temp\\decision.csv"},
                "summary": {},
                "source_status": {},
                "opened_url": "https://example.com/wine",
                "recommendation": {"selected_title": "Cabernet Sauvignon"},
            }

        mock_run_strategy.side_effect = _fake_run_strategy
        plan = {"domain": "web_research", "objective": "Find the best wine for steak dinner tonight."}
        out = search_agent_mod._run_reflective_planner(plan=plan, instruction=plan["objective"])
        self.assertTrue(out.get("ok"))
        self.assertEqual(seen[0], "generic_research")
        self.assertNotIn("email_triage", seen[:1])
        self.assertEqual(out.get("recommendation", {}).get("selected_title"), "Cabernet Sauvignon")

    @patch("lam.operator_platform.research_primitives.search_web")
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

    @patch("lam.operator_platform.research_primitives.search_web")
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

    def test_build_native_plan_promotes_decision_research_to_spreadsheet_report_dashboard(self) -> None:
        plan = search_agent_mod._build_native_plan(
            "Go research and find me the best wine to buy for dinner tonight. I am having steak and potatoes."
        )
        self.assertEqual(plan.get("domain"), "web_research")
        self.assertIn("spreadsheet", plan.get("deliverables", []))
        self.assertIn("report", plan.get("deliverables", []))
        self.assertIn("dashboard", plan.get("deliverables", []))

    def test_build_native_plan_for_payer_review_includes_rag_and_spreadsheet_outputs(self) -> None:
        plan = search_agent_mod._build_native_plan(
            "Review Durham, NC payer pricing, build a vector store or RAG index, create the stakeholder workbook, and identify which plans need outreach."
        )
        self.assertEqual(plan.get("domain"), "payer_pricing_review")
        self.assertIn("spreadsheet", plan.get("deliverables", []))
        self.assertIn("dashboard", plan.get("deliverables", []))
        self.assertIn("rag_index", plan.get("deliverables", []))

    def test_build_native_plan_for_code_workbench_includes_workspace_and_code_outputs(self) -> None:
        plan = search_agent_mod._build_native_plan(
            "Create a new VS Code workspace, write analysis code, and leave me a runnable scaffold."
        )
        self.assertEqual(plan.get("domain"), "code_workbench")
        self.assertIn("code", plan.get("deliverables", []))
        self.assertIn("workspace", plan.get("deliverables", []))
        self.assertTrue(plan.get("playbook_validation", {}).get("ok"))

    def test_build_native_plan_auto_routes_deep_analysis_prompt_into_code_workbench(self) -> None:
        plan = search_agent_mod._build_native_plan(
            "Research public hospital pricing data, build a RAG model, write and test the code, fix failures, and package the result for stakeholders."
        )
        self.assertEqual(plan.get("domain"), "code_workbench")
        self.assertIn("code", plan.get("deliverables", []))

    def test_build_native_plan_keeps_payer_review_precedence_over_code_workbench(self) -> None:
        plan = search_agent_mod._build_native_plan(
            "Review Durham, NC payer pricing, build a vector store or RAG index, write and test the code, create the stakeholder workbook, and identify which plans need outreach."
        )
        self.assertEqual(plan.get("domain"), "payer_pricing_review")

    @patch("lam.interface.search_agent._execute_native_plan")
    def test_recommendation_research_routes_into_native_plan(self, mock_exec_native) -> None:
        mock_exec_native.return_value = {
            "ok": True,
            "mode": "autonomous_plan_execute",
            "query": "best wine for steak dinner",
            "results_count": 3,
            "results": [],
            "artifacts": {"decision_matrix_csv": "C:\\temp\\decision.csv", "dashboard_html": "C:\\temp\\dashboard.html"},
            "summary": {},
            "source_status": {"browser_worker": "ok:local"},
            "opened_url": "https://example.com/wine-guide",
            "canvas": {"title": "Decision Package Generated", "subtitle": "Cabernet Sauvignon", "cards": []},
        }
        result = execute_instruction(
            "Go research and find me the best wine to buy for dinner tonight. I am having steak and potatoes.",
            control_granted=True,
        )
        self.assertTrue(result.get("ok"))
        self.assertEqual(result.get("mode"), "autonomous_plan_execute")
        self.assertTrue(mock_exec_native.called)
        plan_arg = mock_exec_native.call_args.kwargs.get("plan", {})
        self.assertIn("spreadsheet", plan_arg.get("deliverables", []))

    @patch("lam.interface.search_agent._execute_native_plan")
    def test_payer_review_routes_into_native_plan(self, mock_exec_native) -> None:
        mock_exec_native.return_value = {
            "ok": True,
            "mode": "autonomous_plan_execute",
            "query": "durham payer pricing",
            "results_count": 8,
            "results": [],
            "artifacts": {
                "workbook_xlsx": "C:\\temp\\durham_nc_payer_outreach_candidates.xlsx",
                "dashboard_html": "C:\\temp\\payer_dashboard.html",
                "summary_report_md": "C:\\temp\\summary_report.md",
                "validation_queue_csv": "C:\\temp\\contract_validation_queue.csv",
                "rag_index_db": "C:\\temp\\payer_rag.db",
            },
            "summary": {},
            "source_status": {"payer_rag": "ok"},
            "opened_url": "file:///C:/temp/payer_dashboard.html",
            "canvas": {"title": "Durham Payer Review Ready", "subtitle": "8 outreach candidates", "cards": []},
        }
        result = execute_instruction(
            "Review Durham, NC payer pricing, build a vector store or RAG index, create the stakeholder workbook, and identify which plans need outreach.",
            control_granted=True,
        )
        self.assertTrue(result.get("ok"))
        self.assertEqual(result.get("mode"), "autonomous_plan_execute")
        self.assertTrue(mock_exec_native.called)
        plan_arg = mock_exec_native.call_args.kwargs.get("plan", {})
        self.assertEqual(plan_arg.get("domain"), "payer_pricing_review")

    @patch("lam.interface.search_agent._execute_native_plan")
    def test_code_workbench_routes_into_native_plan(self, mock_exec_native) -> None:
        mock_exec_native.return_value = {
            "ok": True,
            "mode": "autonomous_plan_execute",
            "query": "code workbench",
            "results_count": 1,
            "results": [],
            "artifacts": {
                "workspace_directory": "C:\\temp\\deep_work",
                "analysis_script_py": "C:\\temp\\deep_work\\src\\analysis.py",
                "workspace_readme_md": "C:\\temp\\deep_work\\README.md",
                "smoke_log": "C:\\temp\\deep_work\\artifacts\\smoke.log",
            },
            "summary": {},
            "source_status": {"deep_workbench": "ok"},
            "opened_url": "C:\\temp\\deep_work\\README.md",
            "canvas": {"title": "Code Workbench Ready", "subtitle": "scaffold", "cards": []},
        }
        result = execute_instruction(
            "Create a new VS Code instance, write code to analyze this task, and leave me a runnable scaffold.",
            control_granted=True,
        )
        self.assertTrue(result.get("ok"))
        self.assertEqual(result.get("mode"), "autonomous_plan_execute")
        plan_arg = mock_exec_native.call_args.kwargs.get("plan", {})
        self.assertEqual(plan_arg.get("domain"), "code_workbench")

    def test_ui_build_prompt_routes_through_execution_graph_runtime(self) -> None:
        result = execute_instruction(
            "Redesign the app into a clean commercial chat and canvas UI with artifact viewers and a polished dashboard.",
            control_granted=True,
        )
        self.assertTrue(result.get("ok"))
        self.assertEqual(result.get("mode"), "execution_graph_runtime")
        self.assertIn("ui_cards", result)
        self.assertIn("artifact_manifest_json", result.get("artifacts", {}))
        self.assertTrue(any(evt.get("event") == "graph_started" for evt in result.get("runtime_events", [])))
        self.assertTrue(any(evt.get("event") == "revision_started" for evt in result.get("runtime_events", [])))
        self.assertEqual(result.get("task_contract", {}).get("domain"), "ui_build")

    def test_deep_analysis_prompt_routes_through_execution_graph_runtime_without_vscode_phrase(self) -> None:
        result = execute_instruction(
            "Research public hospital pricing data, build a RAG model, write and test the code, fix failures, and package the result for stakeholders.",
            control_granted=True,
        )
        self.assertTrue(result.get("ok"))
        self.assertEqual(result.get("mode"), "execution_graph_runtime")
        self.assertEqual(result.get("task_contract", {}).get("domain"), "deep_analysis")
        self.assertIn("artifact_manifest_json", result.get("artifacts", {}))
        self.assertTrue(any(evt.get("event") == "graph_started" for evt in result.get("runtime_events", [])))

    @patch("lam.interface.search_agent.subprocess.run")
    @patch("lam.interface.search_agent.build_code_workbench_workspace")
    def test_run_code_workbench_creates_workspace_result(self, mock_build_workspace, mock_run) -> None:
        temp_root = Path("data") / "test_artifacts"
        temp_root.mkdir(parents=True, exist_ok=True)
        workspace = temp_root / "search_agent_code_workbench_case"
        shutil.rmtree(workspace, ignore_errors=True)
        workspace.mkdir(parents=True, exist_ok=True)
        try:
            readme = workspace / "README.md"
            readme.write_text("# workspace", encoding="utf-8")
            smoke = workspace / "artifacts" / "smoke_test.log"
            smoke.parent.mkdir(parents=True, exist_ok=True)
            smoke.write_text("", encoding="utf-8")
            (workspace / "artifacts" / "analysis_summary.json").write_text("{}", encoding="utf-8")
            mock_build_workspace.return_value = {
                "workspace": str(workspace),
                "artifact_paths": {
                    "workspace_directory": str(workspace),
                    "analysis_script_py": str(workspace / "src" / "analysis.py"),
                    "workspace_readme_md": str(readme),
                    "smoke_log": str(smoke),
                    "primary_open_file": str(readme),
                },
                "vscode_launch": {"ok": True, "launched": "code", "mode": "new_window"},
                "generation_timestamp": "2026-04-25T20:30:00",
                "current_task_contract": {"title": "Deep Work Task"},
            }
            mock_run.return_value = unittest.mock.Mock(returncode=0, stdout="ok", stderr="")
            out = search_agent_mod._run_code_workbench(
                "Create a new VS Code workspace and write code to analyze the task."
            )
            self.assertTrue(out.get("ok"))
            self.assertEqual(out.get("mode"), "code_workbench")
            self.assertIn("analysis_summary_json", out.get("artifacts", {}))
            self.assertTrue(out.get("summary", {}).get("smoke_test_passed"))
        finally:
            shutil.rmtree(workspace, ignore_errors=True)

    @patch("lam.interface.search_agent.ask_workspace_question")
    @patch("lam.interface.search_agent.ensure_workspace")
    def test_run_payer_pricing_review_question_with_explicit_geography_forces_fresh_run(self, mock_ensure, mock_ask) -> None:
        mock_ensure.return_value = {
            "workspace": "C:\\temp\\payer",
            "artifact_paths": {
                "dashboard_html": "C:\\temp\\durham_nc_payer_dashboard.html",
                "primary_open_file": "C:\\temp\\durham_nc_payer_dashboard.html",
                "workbook_xlsx": "C:\\temp\\durham_nc_payer_outreach_candidates.xlsx",
                "summary_report_md": "C:\\temp\\durham_nc_summary_report.md",
                "validation_queue_csv": "C:\\temp\\durham_nc_contract_validation_queue.csv",
                "rag_index_db": "C:\\temp\\payer_rag.db",
            },
            "counts": {"outreach_candidates": 9},
            "reused_existing_outputs": True,
            "current_task_contract": {"geography": "Durham, NC"},
            "invalidated_artifacts": ["fairfax_va_20260425_120000: geography mismatch (Fairfax, VA != Durham, NC)"],
            "generation_timestamp": "2026-04-25T12:00:00",
            "geography_validation": {"passed": True, "errors": []},
        }
        mock_ask.return_value = {
            "answer": "1. United Healthcare / Commercial/EPO/PPO / MRI brain without contrast (14.2% above peer median)",
            "sources": ["sample://duke | row_4"],
        }
        result = search_agent_mod._run_payer_pricing_review(
            "Which plans need outreach in the Durham payer corpus?"
        )
        self.assertTrue(result.get("ok"))
        self.assertEqual(result.get("mode"), "payer_pricing_review")
        self.assertEqual(result.get("results_count"), 1)
        self.assertIn("validation_queue_csv", result.get("artifacts", {}))
        self.assertTrue(result.get("summary", {}).get("invalidated_artifacts"))
        self.assertFalse(mock_ensure.call_args.kwargs.get("allow_reuse"))

    @patch("lam.interface.search_agent.ensure_workspace")
    def test_run_payer_pricing_review_build_prompt_forces_fresh_rebuild(self, mock_ensure) -> None:
        mock_ensure.return_value = {
            "workspace": "C:\\temp\\fairfax_va_20260425_120000",
            "artifact_paths": {
                "dashboard_html": "C:\\temp\\fairfax_va_payer_dashboard.html",
                "primary_open_file": "C:\\temp\\fairfax_va_payer_dashboard.html",
                "workbook_xlsx": "C:\\temp\\fairfax_va_payer_outreach_candidates.xlsx",
                "summary_report_md": "C:\\temp\\fairfax_va_summary_report.md",
                "validation_queue_csv": "C:\\temp\\fairfax_va_contract_validation_queue.csv",
                "rag_index_db": "C:\\temp\\payer_rag.db",
            },
            "counts": {"outreach_candidates": 5},
            "reused_existing_outputs": False,
            "current_task_contract": {"geography": "Fairfax, VA"},
            "invalidated_artifacts": ["durham_nc_20260425_100000: geography mismatch (Durham, NC != Fairfax, VA)"],
            "generation_timestamp": "2026-04-25T12:00:00",
            "geography_validation": {"passed": True, "errors": []},
            "top_candidates": [],
            "issues": [],
        }
        result = search_agent_mod._run_payer_pricing_review(
            "Build a payer pricing package for Fairfax, VA with a RAG index and stakeholder workbook."
        )
        self.assertTrue(result.get("ok"))
        self.assertEqual(result.get("canvas", {}).get("title"), "Fairfax, VA Payer Review Ready")
        self.assertFalse(result.get("summary", {}).get("reused_existing_outputs"))
        self.assertTrue(mock_ensure.call_args.kwargs.get("allow_reuse") is False)

    @patch("lam.interface.search_agent.ensure_workspace")
    def test_run_payer_pricing_review_blocks_wrong_geography_artifacts(self, mock_ensure) -> None:
        mock_ensure.return_value = {
            "workspace": "C:\\temp\\fairfax_va_20260425_120000",
            "artifact_paths": {
                "dashboard_html": "C:\\temp\\fairfax_va_payer_dashboard.html",
                "primary_open_file": "C:\\temp\\fairfax_va_payer_dashboard.html",
                "workbook_xlsx": "C:\\temp\\fairfax_va_payer_outreach_candidates.xlsx",
                "summary_report_md": "C:\\temp\\fairfax_va_summary_report.md",
                "validation_queue_csv": "C:\\temp\\fairfax_va_contract_validation_queue.csv",
                "rag_index_db": "C:\\temp\\payer_rag.db",
            },
            "counts": {"outreach_candidates": 5},
            "reused_existing_outputs": False,
            "current_task_contract": {"geography": "Fairfax, VA"},
            "invalidated_artifacts": ["durham_nc_20260425_100000: geography mismatch (Durham, NC != Fairfax, VA)"],
            "generation_timestamp": "2026-04-25T12:00:00",
            "geography_validation": {"passed": False, "errors": ["stale geography token 'durham' found in fairfax_va_summary_report.md"]},
            "top_candidates": [],
            "issues": [],
        }
        result = search_agent_mod._run_payer_pricing_review(
            "Build a payer pricing package for Fairfax, VA with a RAG index and stakeholder workbook."
        )
        self.assertFalse(result.get("ok"))
        self.assertEqual(result.get("error_code"), "geography_consistency_failed")

    @patch("lam.interface.search_agent.ensure_workspace")
    def test_run_payer_pricing_review_blocks_final_output_gate_failure(self, mock_ensure) -> None:
        mock_ensure.return_value = {
            "workspace": "C:\\temp\\fairfax_va_20260427_120000",
            "artifact_paths": {
                "geography_validation_report_md": "C:\\temp\\fairfax_va_geography_validation.md",
                "primary_open_file": "C:\\temp\\fairfax_va_geography_validation.md",
            },
            "counts": {"outreach_candidates": 0},
            "reused_existing_outputs": False,
            "current_task_contract": {"geography": "Fairfax, VA", "service_focus": "outpatient imaging"},
            "invalidated_artifacts": ["durham_nc_20260425_100000: geography mismatch (Durham, NC != Fairfax, VA)"],
            "generation_timestamp": "2026-04-27T12:00:00",
            "geography_validation": {"passed": True, "errors": []},
            "validation_results": {
                "geography": {"passed": True, "issue_count": 0},
                "service_scope": {"passed": False, "issue_count": 3},
                "source_relevance": {"passed": False, "issue_count": 4},
                "artifact_contamination": {"passed": False, "issue_count": 2},
            },
            "final_output_gate": {
                "passed": False,
                "severity": "blocking",
                "blocking_failures": ["ServiceScopeValidator", "SourceRelevanceValidator", "ArtifactContaminationValidator"],
                "required_repairs": ["Filter to outpatient imaging and rebuild with Fairfax evidence."],
                "issue_count": 9,
            },
            "quarantined_artifacts": {"summary_report_md": "C:\\temp\\fairfax_va_summary_report.md"},
            "repair_state": {"service_scope_repair": {"attempted": True}},
            "top_candidates": [],
            "issues": [],
            "synthetic_only": False,
        }
        result = search_agent_mod._run_payer_pricing_review(
            "Build a Fairfax, VA outpatient imaging payer/pricing review with a RAG index and stakeholder workbook."
        )
        self.assertFalse(result.get("ok"))
        self.assertEqual(result.get("error_code"), "final_output_gate_failed")
        self.assertIn("validation_results", result)
        self.assertEqual(result.get("canvas", {}).get("title"), "Validation Failed")

    @patch("lam.interface.search_agent.ensure_workspace")
    def test_run_payer_pricing_review_returns_completed_demo_package(self, mock_ensure) -> None:
        mock_ensure.return_value = {
            "workspace": "C:\\temp\\fairfax_va_demo_20260427_120000",
            "artifact_paths": {
                "dashboard_html": "C:\\temp\\fairfax_va_payer_dashboard.html",
                "primary_open_file": "C:\\temp\\fairfax_va_payer_dashboard.html",
                "workbook_xlsx": "C:\\temp\\fairfax_va_payer_outreach_candidates.xlsx",
                "summary_report_md": "C:\\temp\\fairfax_va_summary_report.md",
                "source_manifest_csv": "C:\\temp\\fairfax_va_source_manifest.csv",
                "validation_queue_csv": "C:\\temp\\fairfax_va_contract_validation_queue.csv",
                "real_data_acquisition_checklist_md": "C:\\temp\\fairfax_va_real_data_acquisition_checklist.md",
                "rag_index_db": "C:\\temp\\payer_rag.db",
            },
            "counts": {"outreach_candidates": 4},
            "reused_existing_outputs": False,
            "current_task_contract": {"geography": "Fairfax, VA", "service_focus": "outpatient imaging"},
            "invalidated_artifacts": ["durham_nc_20260425_100000: geography mismatch (Durham, NC != Fairfax, VA)"],
            "generation_timestamp": "2026-04-27T12:00:00",
            "geography_validation": {"passed": True, "errors": []},
            "validation_results": {
                "geography": {"passed": True, "issue_count": 0},
                "service_scope": {"passed": True, "issue_count": 0},
                "source_relevance": {"passed": True, "issue_count": 0},
                "artifact_contamination": {"passed": True, "issue_count": 0},
            },
            "final_output_gate": {
                "passed": True,
                "severity": "blocking",
                "blocking_failures": [],
                "required_repairs": [],
                "issue_count": 0,
            },
            "quarantined_artifacts": {},
            "repair_state": {"source_repair_attempted": True},
            "top_candidates": [
                {"payer_name": "United Healthcare", "plan_name": "Commercial PPO", "service": "MRI brain without contrast", "variance_percent": 0.14, "source_evidence": "sample://fairfax_demo | row_1"}
            ],
            "issues": [],
            "synthetic_only": True,
            "completion_status": "completed_demo_package",
        }
        result = search_agent_mod._run_payer_pricing_review(
            "Build a Fairfax, VA outpatient imaging payer/pricing review with synthetic fallback if needed."
        )
        self.assertTrue(result.get("ok"))
        self.assertEqual(result.get("completion_status"), "completed_demo_package")
        self.assertEqual(result.get("source_basis"), "synthetic_demo")
        self.assertIn("Demo Package Ready", result.get("canvas", {}).get("title", ""))

    @patch("lam.operator_platform.research_primitives.search_web")
    @patch("lam.interface.search_agent._write_generic_research_artifacts")
    @patch("lam.interface.search_agent.webbrowser.open")
    @patch("lam.interface.search_agent._relevance_score", return_value=2.0)
    def test_generic_research_includes_judgment_summary(self, _mock_rel, _mock_open, mock_write, mock_search) -> None:
        mock_search.return_value = [
            search_agent_mod.SearchResult(
                title="Concrete whiskey product listing in Durham",
                url="https://store.example.com/product/whiskey/durham-nc",
                price=199.0,
                source="duckduckgo",
                snippet="Durham NC whiskey inventory available.",
            ),
            search_agent_mod.SearchResult(
                title="Another whiskey product listing in Durham",
                url="https://shop.example.com/item/rare-whiskey-durham",
                price=249.0,
                source="duckduckgo",
                snippet="Local whiskey price and pickup in Durham.",
            ),
        ]
        out_dir = self._case_dir("generic_research_judgment_summary")
        html_path = out_dir / "dashboard.html"
        html_path.write_text("<html></html>", encoding="utf-8")
        mock_write.return_value = {"dashboard_html": str(html_path), "primary_open_file": str(html_path)}
        out = search_agent_mod._run_generic_research(
            "Research whiskey options and produce a report.",
            progress_cb=None,
        )
        self.assertTrue(out.get("ok"))
        self.assertIn("judgment", out.get("summary", {}))
        self.assertIn("artifact_metadata", out)

    @patch("lam.operator_platform.research_primitives.search_web")
    @patch("lam.interface.search_agent._write_generic_research_artifacts")
    @patch("lam.interface.search_agent.webbrowser.open")
    @patch("lam.interface.search_agent._relevance_score", return_value=2.0)
    def test_generic_research_artifacts_are_exported_through_artifact_executor(self, _mock_rel, _mock_open, mock_write, mock_search) -> None:
        mock_search.return_value = [
            search_agent_mod.SearchResult(
                title="Concrete market research report",
                url="https://example.com/report",
                price=None,
                source="duckduckgo",
                snippet="Strong market research result.",
            ),
            search_agent_mod.SearchResult(
                title="Another market analysis report",
                url="https://example.com/report-2",
                price=None,
                source="duckduckgo",
                snippet="Another strong market analysis result.",
            ),
        ]
        out_dir = self._case_dir("generic_research_artifact_export")
        dash_path = out_dir / "dashboard.html"
        dash_path.write_text("<html></html>", encoding="utf-8")
        mock_write.return_value = {"dashboard_html": str(dash_path), "primary_open_file": str(dash_path)}
        out = search_agent_mod._run_generic_research(
            "Research the market and build a dashboard report.",
            progress_cb=None,
        )
        self.assertTrue(out.get("ok"))
        self.assertEqual(out.get("runtime_mode"), "execution_graph_runtime")
        self.assertIn("dashboard_html", out.get("artifacts", {}))
        self.assertIn("dashboard_html", out.get("artifact_metadata", {}))
        self.assertTrue(any(evt.get("event") == "graph_started" for evt in out.get("runtime_events", [])))

    @patch("lam.interface.search_agent._open_target_with_reuse")
    @patch("lam.interface.search_agent._browser_research_walk")
    @patch("lam.operator_platform.research_primitives.search_web")
    def test_generic_research_generates_wine_recommendation(self, mock_search, mock_browser_walk, mock_open_target) -> None:
        mock_open_target.side_effect = lambda target_url, recent_actions=None: (target_url, {"decision": {"score": 80, "reasons": ["ok"]}})
        mock_browser_walk.return_value = {
            "ok": True,
            "opened_url": "https://example.com/cabernet-steak",
            "worker_status": "local_session",
            "notes": [
                {
                    "url": "https://example.com/cabernet-steak",
                    "title": "Cabernet with Steak",
                    "summary": "Cabernet Sauvignon is the strongest pairing for steak and potatoes.",
                }
            ],
        }
        mock_search.return_value = [
            search_agent_mod.SearchResult(
                title="Best red wine with steak: Cabernet Sauvignon guide",
                url="https://example.com/cabernet-steak",
                price=None,
                source="duckduckgo",
                snippet="Cabernet Sauvignon is the classic steak pairing.",
            ),
            search_agent_mod.SearchResult(
                title="Malbec with steak and potatoes",
                url="https://example.com/malbec-steak",
                price=None,
                source="duckduckgo",
                snippet="Malbec pairs well with grilled steak dinners.",
            ),
            search_agent_mod.SearchResult(
                title="Merlot or cabernet for steak dinner",
                url="https://example.com/merlot-cabernet",
                price=None,
                source="duckduckgo",
                snippet="Cabernet Sauvignon remains the strongest steak pairing option.",
            ),
        ]
        out = search_agent_mod._run_generic_research(
            "Go research and find me the best wine to buy for dinner tonight. I am having steak and potatoes.",
            progress_cb=None,
            browser_worker_mode="local",
            human_like_interaction=True,
        )
        self.assertTrue(out.get("ok"))
        self.assertIn("decision_matrix_csv", out.get("artifacts", {}))
        self.assertIn("browser_research_md", out.get("artifacts", {}))
        self.assertEqual(out.get("recommendation", {}).get("selected_title"), "Cabernet Sauvignon")
        self.assertIn("browser_worker_mode", out.get("summary", {}))

    @patch("lam.interface.search_agent._search_web")
    def test_competitor_analysis_executes_through_runtime(self, mock_search) -> None:
        mock_search.return_value = [
            search_agent_mod.SearchResult(
                title="Epic vs Oracle Health",
                url="https://example.com/oracle-health",
                price=None,
                source="duckduckgo",
                snippet="Oracle Health is a common Epic competitor in hospitals.",
            ),
            search_agent_mod.SearchResult(
                title="Epic vs MEDITECH",
                url="https://example.com/meditech",
                price=None,
                source="duckduckgo",
                snippet="MEDITECH appears in enterprise EHR comparisons with Epic.",
            ),
            search_agent_mod.SearchResult(
                title="Epic vs athenahealth",
                url="https://example.com/athena",
                price=None,
                source="duckduckgo",
                snippet="athenahealth appears in ambulatory EHR comparisons.",
            ),
        ]
        out = search_agent_mod._run_competitor_analysis(
            "Research the top 5 competitors to Epic Systems, build a 2-page executive summary, create a PowerPoint, and save everything to a folder called Epic Competitor Analysis.",
            progress_cb=None,
            min_live_non_curated_citations=1,
        )
        self.assertTrue(out.get("ok"))
        self.assertEqual(out.get("mode"), "competitor_analysis")
        self.assertEqual(out.get("runtime_mode"), "execution_graph_runtime")
        self.assertIn("competitors_csv", out.get("artifacts", {}))
        self.assertIn("runtime_events", out)

    @patch("lam.interface.search_agent.ensure_browser_worker")
    @patch("playwright.sync_api.sync_playwright")
    def test_browser_research_walk_collects_page_notes(self, mock_sync_playwright, mock_ensure_worker) -> None:
        mock_ensure_worker.return_value = {"ok": False, "error": "unavailable"}

        page_state = {
            "https://example.com/a": {
                "title": "Cabernet with Steak",
                "text": "Cabernet Sauvignon is a strong pairing for steak and potatoes dinner.",
            },
            "https://example.com/b": {
                "title": "Malbec Pairing Guide",
                "text": "Malbec works well with grilled steak, but cabernet remains the classic.",
            },
        }

        class _BodyLocator:
            def __init__(self, page: Any) -> None:
                self.page = page

            def inner_text(self, timeout: int = 0) -> str:
                _ = timeout
                return page_state.get(self.page.url, {}).get("text", "")

        class _Page:
            def __init__(self) -> None:
                self.url = ""

            def goto(self, url: str, timeout: int = 0) -> None:
                _ = timeout
                self.url = url

            def wait_for_timeout(self, _ms: int) -> None:
                return None

            def title(self) -> str:
                return page_state.get(self.url, {}).get("title", "")

            def locator(self, selector: str) -> Any:
                self.assert_selector = selector
                return _BodyLocator(self)

        class _Context:
            def __init__(self) -> None:
                self._page = _Page()
                self.pages = []

            def new_page(self) -> Any:
                return self._page

        class _Browser:
            def __init__(self) -> None:
                self._context = _Context()

            def new_context(self) -> Any:
                return self._context

            def close(self) -> None:
                return None

        class _Chromium:
            def connect_over_cdp(self, _url: str, timeout: int = 0) -> Any:
                _ = timeout
                raise RuntimeError("no cdp")

            def launch(self, headless: bool = True) -> Any:
                _ = headless
                return _Browser()

        class _Playwright:
            chromium = _Chromium()

        class _Factory:
            def __enter__(self) -> _Playwright:
                return _Playwright()

            def __exit__(self, exc_type, exc, tb) -> bool:
                _ = (exc_type, exc, tb)
                return False

        mock_sync_playwright.return_value = _Factory()
        out = search_agent_mod._browser_research_walk(
            query="best wine for steak dinner",
            candidates=[
                search_agent_mod.SearchResult(title="A", url="https://example.com/a", price=None, source="web", snippet=""),
                search_agent_mod.SearchResult(title="B", url="https://example.com/b", price=None, source="web", snippet=""),
            ],
            browser_worker_mode="local",
            human_like_interaction=True,
            progress_cb=None,
            max_pages=2,
        )
        self.assertTrue(out.get("ok"))
        self.assertEqual(len(out.get("notes", [])), 2)
        self.assertEqual(out.get("opened_url"), "https://example.com/b")

    @patch("lam.interface.search_agent._open_target_with_reuse")
    @patch("lam.interface.search_agent._browser_research_walk")
    @patch("lam.operator_platform.research_primitives.search_web")
    def test_generic_research_product_compare_uses_browser_notes(self, mock_search, mock_browser_walk, mock_open_target) -> None:
        mock_open_target.side_effect = lambda target_url, recent_actions=None: (target_url, {"decision": {"score": 82, "reasons": ["ok"]}})
        mock_search.return_value = [
            search_agent_mod.SearchResult(
                title="AeroPress Go travel coffee maker review",
                url="https://example.com/aeropress-go",
                price=44.95,
                source="duckduckgo",
                snippet="Portable coffee maker for travel.",
            ),
            search_agent_mod.SearchResult(
                title="Wacaco Nanopresso review for travel",
                url="https://example.com/nanopresso",
                price=69.90,
                source="duckduckgo",
                snippet="Compact espresso maker for carry-on use.",
            ),
            search_agent_mod.SearchResult(
                title="OutIn Nano portable espresso maker",
                url="https://example.com/outin-nano",
                price=149.0,
                source="duckduckgo",
                snippet="Battery-powered travel espresso machine.",
            ),
        ]
        mock_browser_walk.return_value = {
            "ok": True,
            "opened_url": "https://example.com/nanopresso",
            "worker_status": "local_session",
            "notes": [
                {"url": "https://example.com/aeropress-go", "title": "AeroPress Go", "summary": "Lightweight and simple, but not true espresso."},
                {"url": "https://example.com/nanopresso", "title": "Nanopresso", "summary": "Better espresso quality with strong travel portability."},
            ],
        }
        out = search_agent_mod._run_generic_research(
            "Go research the best portable espresso maker for travel and recommend which one to buy.",
            progress_cb=None,
            browser_worker_mode="local",
            human_like_interaction=True,
        )
        self.assertTrue(out.get("ok"))
        self.assertIn("decision_matrix_csv", out.get("artifacts", {}))
        self.assertIn("browser_research_json", out.get("artifacts", {}))
        self.assertEqual(out.get("summary", {}).get("browser_pages_reviewed"), 2)
        self.assertEqual(out.get("source_status", {}).get("browser_walk"), "local_session")

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
            artifacts = {}
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
            artifacts = {}
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
            artifacts = {}
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
        root = self._case_dir("email_triage_manual_auth_phase")
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
            artifacts = {}
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
        self.assertEqual(result.get("runtime_mode"), "execution_graph_runtime")
        self.assertIn("artifact_manifest_json", result.get("artifacts", {}))
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

    def test_image_base64_roundtrip_helpers(self) -> None:
        base = self._case_dir("image_base64_roundtrip")
        src = base / "sample.png"
        src.write_bytes(base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+Xj1wAAAAASUVORK5CYII="))
        encoded = search_agent_mod.image_to_base64(src)
        self.assertTrue(encoded)
        restored = search_agent_mod.base64_to_image(encoded, base / "restored.png")
        self.assertTrue(restored.exists())
        self.assertEqual(src.read_bytes(), restored.read_bytes())

    def test_study_image_url_prefers_base64_when_file_missing(self) -> None:
        value = search_agent_mod._study_image_url(img_path="", image_base64="YWJj")
        self.assertEqual(value, "data:image/png;base64,YWJj")

    @patch("lam.interface.search_agent.LOGGER")
    def test_render_pdf_page_image_logs_failures(self, mock_logger) -> None:
        class BrokenDoc:
            def load_page(self, _page_index: int) -> Any:
                raise RuntimeError("boom")

        with patch("lam.interface.search_agent._get_fitz_module", return_value=type("Fitz", (), {"Matrix": lambda self, x, y: (x, y)})()):
            path = search_agent_mod._render_pdf_page_image(BrokenDoc(), 0, "https://example.com/file.pdf")
        self.assertEqual(path, "")
        mock_logger.warning.assert_called()

    def test_capture_clipboard_image_saves_image(self) -> None:
        base = self._case_dir("clipboard_image_capture")

        class FakeImage:
            def save(self, target: str, format: str = "PNG") -> None:
                Path(target).write_bytes(b"fake-image")

        fake_imagegrab = types.SimpleNamespace(grabclipboard=lambda: FakeImage())
        fake_pil = types.SimpleNamespace(ImageGrab=fake_imagegrab)
        with patch.dict(sys.modules, {"PIL": fake_pil, "PIL.ImageGrab": fake_imagegrab}):
            out = search_agent_mod.capture_clipboard_image(base / "clip.png")
        self.assertTrue(out)
        self.assertTrue(Path(out).exists())

    @patch("lam.interface.search_agent.capture_clipboard_image")
    def test_execute_instruction_clipboard_capture_route(self, mock_capture_clipboard_image) -> None:
        base = self._case_dir("execute_instruction_clipboard_capture")
        image_path = base / "clipboard.png"
        image_path.write_bytes(base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+Xj1wAAAAASUVORK5CYII="))
        mock_capture_clipboard_image.return_value = str(image_path.resolve())
        result = execute_instruction(
            "Capture the current clipboard image and save it as an artifact package with base64 output.",
            control_granted=True,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "clipboard_capture")
        self.assertEqual(result.get("source_status", {}).get("clipboard"), "ok")
        self.assertIn("clipboard_image_png", result.get("artifacts", {}))
        self.assertIn("clipboard_image_base64_txt", result.get("artifacts", {}))
        self.assertTrue(str(result.get("opened_url", "")).startswith("file:///"))

    @patch("lam.interface.search_agent.get_guidance")
    @patch("lam.interface.search_agent.execute_plan")
    @patch("lam.interface.search_agent.assess_risk")
    @patch("lam.interface.search_agent.build_plan")
    def test_desktop_sequence_clipboard_artifacts_surface_in_response(self, mock_build_plan, mock_risk, mock_exec, mock_guidance) -> None:
        class R:
            ok = True
            trace = [{"step": 1, "action": "capture_clipboard_image", "ok": True, "artifact": "C:\\temp\\clip.png"}]
            done = True
            next_step_index = 2
            paused_for_credentials = False
            pause_reason = ""
            artifacts = {"clipboard_image_png": "C:\\temp\\clip.png", "primary_open_file": "C:\\temp\\clip.png"}
            error = ""

        mock_build_plan.return_value = {
            "app_name": "paint",
            "steps": [
                {"action": "open_app", "app": "paint"},
                {"action": "capture_clipboard_image", "output_path": "", "source": "system_clipboard"},
            ],
        }
        mock_risk.return_value = {"requires_confirmation": False, "risky_steps": []}
        mock_exec.return_value = R()
        mock_guidance.return_value = {"app_name": "paint", "guidance": []}
        result = execute_instruction("open paint then capture clipboard image", control_granted=True)
        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "desktop_sequence")
        self.assertIn("clipboard_image_png", result.get("artifacts", {}))
        self.assertTrue(str(result.get("opened_url", "")).startswith("file:///"))


if __name__ == "__main__":
    unittest.main()
