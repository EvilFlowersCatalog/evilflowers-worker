import pytest
from pydantic import ValidationError

from evilflowers_worker.contract import (
    MAX_ERROR_LENGTH,
    ExtractRequest,
    JobReport,
    LayoutRegion,
    Paragraph,
    ParagraphsJob,
    Region,
    RegionsJob,
    RunState,
    StartRun,
    batch_job,
    dump,
    job_kind,
)

COMMON = {"run_id": "run-1", "source": "a.pdf", "output_dir": "out"}
BOX = {"x0": 0.1, "y0": 0.2, "x1": 0.3, "y1": 0.4}


def test_an_extract_request_without_steps_is_a_full_pass():
    request = ExtractRequest.model_validate({"run_id": "r", "source": "a.pdf", "output_dir": "out"})
    assert request.steps == []


def test_an_extract_request_carries_a_step_subset():
    request = ExtractRequest.model_validate(
        {"run_id": "r", "source": "a.pdf", "output_dir": "out", "steps": ["figures", "tables"]}
    )
    assert request.steps == ["figures", "tables"]
    assert request.kwargs()["steps"] == ["figures", "tables"]


def test_an_unknown_step_is_refused():
    with pytest.raises(ValidationError):
        ExtractRequest.model_validate({"run_id": "r", "source": "a.pdf", "output_dir": "out", "steps": ["outline"]})


def test_a_full_start_sends_no_steps_and_a_step_rerun_sends_its_subset():
    full = StartRun(page_count=1, page_sizes=[[595.0, 842.0]], jobs=["figures", "paragraphs"])
    assert "steps" not in dump(full)
    rerun = StartRun(page_count=1, page_sizes=[[595.0, 842.0]], jobs=["figures"], steps=["figures"])
    assert dump(rerun)["steps"] == ["figures"]


def test_a_region_without_a_decision_sends_no_decision_field():
    region = Region(kind="figure", page=1, seq=0, bbox=BOX)
    assert set(dump(region)) == {"kind", "page", "seq", "bbox", "provenance"}


def test_a_regions_decision_fields_reach_the_payload_and_zero_survives():
    region = Region(
        kind="table", page=2, seq=1, bbox=BOX, band="review", p=0.0, score=0.42, decision="table",
        decider_model="strands-decider-2B-qwen3.5-v1-2610@216d1ccb", review_outcome="rewritten",
        grounded_share=1.0, signals={"rows": 4},
    )  # fmt: skip
    payload = dump(region)
    assert payload["p"] == 0.0 and payload["band"] == "review" and payload["signals"] == {"rows": 4}
    assert payload["review_outcome"] == "rewritten" and payload["grounded_share"] == 1.0


@pytest.mark.parametrize(
    "bad", [{"band": "maybe"}, {"p": 1.5}, {"score": -0.1}, {"grounded_share": 2}, {"decision": "x" * 101}]
)
def test_a_regions_decision_fields_follow_the_catalogs_limits(bad):
    with pytest.raises(ValidationError):
        Region(kind="figure", page=1, seq=0, bbox=BOX, **bad)


def test_batch_job_names_follow_the_catalogs_rules():
    assert batch_job("equations", 1, 8) == "equations:0001-0008"
    assert job_kind("equations:0001-0008") == "equations"
    assert job_kind("paragraphs") == "paragraphs"
    with pytest.raises(ValueError, match="30"):
        batch_job("a_kind_with_a_long_name", 1, 8)


@pytest.mark.parametrize("name", ["Figures", "figures:1", "fig/ures", "", "x" * 31])
def test_a_job_name_the_catalog_would_refuse_is_rejected(name):
    with pytest.raises(ValidationError):
        StartRun(page_count=1, page_sizes=[[1, 1]], jobs=[name])


def test_wire_form_leaves_out_none_at_the_top_but_keeps_empty_lists_and_nested_nones():
    message = RegionsJob(**COMMON, regions=[{"page": 1, "seq": 0, "bbox": BOX, "caption": None}])
    assert message.kwargs() == {**COMMON, "regions": [{"page": 1, "seq": 0, "bbox": BOX, "caption": None}]}
    assert RegionsJob(**COMMON).kwargs()["regions"] == []
    assert ParagraphsJob(**COMMON, attempt=3, outline=[]).kwargs() == {**COMMON, "attempt": 3, "outline": []}
    assert "outline" not in ParagraphsJob(**COMMON).kwargs()


def test_outline_entries_go_as_json_lists():
    assert ParagraphsJob(**COMMON, outline=[(1, "Intro", 2)]).kwargs()["outline"] == [[1, "Intro", 2]]


def test_consumers_ignore_fields_a_newer_producer_adds():
    request = ExtractRequest.model_validate({**COMMON, "pipeline_version": 2, "priority": "high"})
    assert request.pipeline_version == 2
    assert "priority" not in request.kwargs()
    assert LayoutRegion.model_validate({"page": 1, "seq": 0, "bbox": BOX, "kind": "figure"}).page == 1


def test_messages_are_validated_at_the_edge():
    with pytest.raises(ValidationError):
        ExtractRequest.model_validate({**COMMON, "pipeline_version": "second"})
    with pytest.raises(ValidationError):
        LayoutRegion(page=0, seq=0, bbox=BOX)


def test_a_job_report_truncates_its_error():
    assert len(JobReport(status="failed", error="x" * 5000).error) == MAX_ERROR_LENGTH
    assert JobReport(status="done", error="").error is None


def test_a_paragraphs_decision_fields_are_optional_and_zero_survives():
    plain = Paragraph(page=1, seq=0, bbox=BOX, text="t")
    assert not {"decision", "p", "decider_model"} & set(dump(plain))
    judged = Paragraph(page=1, seq=0, bbox=BOX, text="t", decision="caption", p=0.0, decider_model="laya@80a1")
    assert {k: dump(judged)[k] for k in ("decision", "p", "decider_model")} == {
        "decision": "caption", "p": 0.0, "decider_model": "laya@80a1"
    }  # fmt: skip


@pytest.mark.parametrize("bad", [{"p": 1.5}, {"p": -0.1}, {"decision": "x" * 101}, {"decider_model": "x" * 201}])
def test_a_paragraphs_decision_fields_follow_the_catalogs_limits(bad):
    with pytest.raises(ValidationError):
        Paragraph(page=1, seq=0, bbox=BOX, text="t", **bad)


def test_a_paragraph_embedding_must_have_the_catalogs_dimensions():
    with pytest.raises(ValidationError, match="1024"):
        Paragraph(page=1, seq=0, bbox=BOX, text="t", embedding=[0.0] * 3)


@pytest.mark.parametrize(
    ("state", "attempt", "reason"),
    [
        (RunState(status="running", jobs={"figures": "pending"}, attempt=2), 2, None),
        (RunState(status="running", jobs={"figures": "pending"}, attempt=2), None, None),
        (RunState(status="running", jobs={"figures": "pending"}, attempt=3), 2, "superseded"),
        (RunState(status="done", jobs={"figures": "done"}, attempt=2), 2, "not_running"),
        (RunState(status="running", jobs={"figures": "done"}, attempt=2), 2, "already_done"),
        # A catalog that predates EvilFlowersCatalog#140 has no attempt.
        (RunState(status="running", jobs={"figures": "pending"}), 2, None),
    ],
)
def test_skip_reason(state, attempt, reason):
    assert state.skip_reason("figures", attempt) == reason
