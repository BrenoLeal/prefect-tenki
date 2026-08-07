from types import SimpleNamespace

import pytest
from live_worker_smoke import _select_workspace
from tenki import IdentityWorkspace


def identity(*workspaces: IdentityWorkspace) -> SimpleNamespace:
    return SimpleNamespace(workspaces=workspaces)


def test_select_workspace_uses_requested_accessible_workspace():
    workspace = IdentityWorkspace(id="workspace-2", name="Second")

    selected = _select_workspace(
        identity(
            IdentityWorkspace(id="workspace-1", name="First"),
            workspace,
        ),
        workspace.id,
    )

    assert selected == "workspace-2"


def test_select_workspace_auto_selects_only_workspace():
    selected = _select_workspace(
        identity(IdentityWorkspace(id="workspace-1", name="Only")),
        None,
    )

    assert selected == "workspace-1"


def test_select_workspace_rejects_inaccessible_requested_workspace():
    with pytest.raises(RuntimeError, match="not accessible"):
        _select_workspace(
            identity(IdentityWorkspace(id="workspace-1", name="Only")),
            "workspace-missing",
        )


def test_select_workspace_requires_choice_when_multiple_are_accessible():
    with pytest.raises(RuntimeError, match="More than one Tenki workspace"):
        _select_workspace(
            identity(
                IdentityWorkspace(id="workspace-1", name="First"),
                IdentityWorkspace(id="workspace-2", name="Second"),
            ),
            None,
        )


def test_select_workspace_rejects_identity_without_workspaces():
    with pytest.raises(RuntimeError, match="No Tenki workspace"):
        _select_workspace(identity(), None)
