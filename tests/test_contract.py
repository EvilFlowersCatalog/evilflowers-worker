import pytest
from pydantic import ValidationError

from evilflowers_worker.contract import (
    MAX_ERROR_LENGTH,
    ExtractRequest,
    JobReport,
    LayoutRegion,
    Paragraph,
    ParagraphsJob,
    RegionsJob,
    RunState,
    StartRun,
    batch_job,
    job_kind,
)

COMMON = {"run_id": "run-1", "source": "a.pdf", "output_dir": "out"}
BOX = {"x0": 0.1, "y0": 0.2, "x1": 0.3, "y1": 0.4}


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
