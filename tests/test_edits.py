"""Tests for cjm_context_graph_layer.edits — spine-edit vocabulary + projection.

Projected from the edits notebook's three test cells at the c25780e8 flip."""
import pytest

from cjm_context_graph_layer.edits import (SpineEdit, SpineEditError, SpineUnit,
                                           project_effective_spine, resolve_active)


def test_vocabulary_and_supersession():
    with pytest.raises(SpineEditError):
        SpineEdit("e0", "merge")

    assert resolve_active(["a", "b", "c"], [("c", "a")]) == {"b", "c"}
    # chain: c supersedes b supersedes a -> only c active
    assert resolve_active(["a", "b", "c"], [("b", "a"), ("c", "b")]) == {"c"}

    e = SpineEdit("e1", "replace_text", ["s1"], {"text": "fixed"}, created_at=2.0)
    assert SpineEdit.from_dict(e.to_dict()) == e


def test_projection_prune_and_replace_latest_wins():
    spine = [SpineUnit("s1", "hello world"), SpineUnit("s2", ""), SpineUnit("s3", "the end")]
    out = project_effective_spine(spine, [SpineEdit("p1", "prune", ["s2"], created_at=1.0)])
    assert [u.id for u in out] == ["s1", "s3"], "pruned position dropped"
    assert [u.text for u in spine] == ["hello world", "", "the end"], "input not mutated"

    out = project_effective_spine(spine, [
        SpineEdit("r1", "replace_text", ["s1"], {"text": "first"}, created_at=1.0),
        SpineEdit("r2", "replace_text", ["s1"], {"text": "second"}, created_at=2.0),
    ])
    assert out[0].text == "second", "latest replace wins via ordering"


def test_boundary_shift_push_pull_strict_and_composition():
    # stripped-text corpus convention (the real segmentation shape, 58b2e0a0)
    spine = [SpineUnit("s1", "Mr. Gorbachev, tear"), SpineUnit("s2", "down this wall.")]
    out = project_effective_spine(spine, [SpineEdit("b1", "boundary_shift", [],
        {"boundary_after": "s1", "text": "tear", "direction": "push"}, created_at=1.0)])
    assert out[0].text == "Mr. Gorbachev," and out[1].text == "tear down this wall."

    out = project_effective_spine(spine, [SpineEdit("b2", "boundary_shift", [],
        {"boundary_after": "s1", "text": "down", "direction": "pull"}, created_at=1.0)])
    assert out[0].text == "Mr. Gorbachev, tear down" and out[1].text == "this wall."

    # empty-neighbor (the falsified-D14 / FA-starvation class): both sides clean
    starved = [SpineUnit("s1", "largest naval battle in history"), SpineUnit("s2", "")]
    out = project_effective_spine(starved, [SpineEdit("b3", "boundary_shift", [],
        {"boundary_after": "s1", "text": "in history", "direction": "push"}, created_at=1.0)])
    assert out[0].text == "largest naval battle" and out[1].text == "in history"

    # space-carrying texts normalize to the same single-space junction
    spaced = [SpineUnit("s1", "the art of war "), SpineUnit("s2", "is of vital importance")]
    out = project_effective_spine(spaced, [SpineEdit("b4", "boundary_shift", [],
        {"boundary_after": "s1", "text": "war", "direction": "push"}, created_at=1.0)])
    assert out[0].text == "the art of" and out[1].text == "war is of vital importance"

    # strict: moved words absent at the boundary -> loud
    with pytest.raises(SpineEditError):
        project_effective_spine(spine, [SpineEdit("b5", "boundary_shift", [],
            {"boundary_after": "s1", "text": "XYZ", "direction": "push"}, created_at=1.0)])
    # empty moved text -> loud
    with pytest.raises(SpineEditError):
        project_effective_spine(spine, [SpineEdit("b6", "boundary_shift", [],
            {"boundary_after": "s1", "text": "  ", "direction": "push"}, created_at=1.0)])

    # composition: a shift after a replace sees the replaced text
    out = project_effective_spine(spine, [
        SpineEdit("r1", "replace_text", ["s1"], {"text": "AB CD"}, created_at=1.0),
        SpineEdit("b7", "boundary_shift", [], {"boundary_after": "s1", "text": "CD", "direction": "push"}, created_at=2.0),
    ])
    assert out[0].text == "AB" and out[1].text == "CD down this wall."
