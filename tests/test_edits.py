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
    spine = [SpineUnit("s1", "the art of war "), SpineUnit("s2", "is of vital importance")]
    out = project_effective_spine(spine, [SpineEdit("b1", "boundary_shift", [],
        {"boundary_after": "s1", "text": "war ", "direction": "push"}, created_at=1.0)])
    assert out[0].text == "the art of " and out[1].text == "war is of vital importance"

    out = project_effective_spine(spine, [SpineEdit("b2", "boundary_shift", [],
        {"boundary_after": "s1", "text": "is ", "direction": "pull"}, created_at=1.0)])
    assert out[0].text == "the art of war is " and out[1].text == "of vital importance"

    # strict: moved text absent at the boundary -> loud
    with pytest.raises(SpineEditError):
        project_effective_spine(spine, [SpineEdit("b3", "boundary_shift", [],
            {"boundary_after": "s1", "text": "XYZ", "direction": "push"}, created_at=1.0)])

    # composition: replace then shift sees the replaced text
    out = project_effective_spine(spine, [
        SpineEdit("r1", "replace_text", ["s1"], {"text": "AB CD "}, created_at=1.0),
        SpineEdit("b4", "boundary_shift", [], {"boundary_after": "s1", "text": "CD ", "direction": "push"}, created_at=2.0),
    ])
    assert out[0].text == "AB " and out[1].text == "CD is of vital importance"
