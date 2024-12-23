import logging

import pytest

from iceaxe.__tests__.conf_models import ArtifactDemo, UserDemo
from iceaxe.modifications import (
    MODIFICATION_TRACKER_VERBOSITY,
    Modification,
    ModificationTracker,
)


@pytest.fixture
def tracker():
    """Create a fresh ModificationTracker for each test."""
    return ModificationTracker(known_first_party=["test_modifications"])


@pytest.fixture
def demo_instance():
    """Create a demo model instance for testing."""
    return UserDemo(id=1, name="test", email="test@example.com")


def test_get_current_stack_trace():
    """Test that get_current_stack_trace returns both traces."""
    full_trace, user_trace = Modification.get_current_stack_trace()

    assert isinstance(full_trace, str)
    assert isinstance(user_trace, str)
    assert "test_modifications.py" in user_trace
    assert len(full_trace) >= len(user_trace)


def test_track_modification_new_instance(
    tracker: ModificationTracker, demo_instance: UserDemo
):
    """Test tracking a new modification."""
    tracker.track_modification(demo_instance)

    instance_id = id(demo_instance)
    assert instance_id in tracker.modified_models

    modification = tracker.modified_models[instance_id]
    assert modification.instance == demo_instance
    assert "test_modifications.py" in modification.user_stack_trace


def test_track_modification_duplicate(
    tracker: ModificationTracker, demo_instance: UserDemo
):
    """Test that tracking the same instance twice only records it once."""
    tracker.track_modification(demo_instance)
    tracker.track_modification(demo_instance)

    assert len(tracker.modified_models) == 1


def test_clear_status_single(tracker: ModificationTracker, demo_instance: UserDemo):
    """Test committing a single model."""
    tracker.track_modification(demo_instance)
    tracker.clear_status([demo_instance])

    assert id(demo_instance) not in tracker.modified_models


def test_clear_status_partial(tracker: ModificationTracker):
    """Test committing some but not all models."""
    instance1 = UserDemo(id=1, name="test1", email="test1@example.com")
    instance2 = UserDemo(id=2, name="test2", email="test2@example.com")

    tracker.track_modification(instance1)
    tracker.track_modification(instance2)
    tracker.clear_status([instance1])

    assert id(instance1) not in tracker.modified_models
    assert id(instance2) in tracker.modified_models
    assert tracker.modified_models[id(instance2)].instance == instance2


@pytest.mark.parametrize("verbosity", ["ERROR", "WARNING", "INFO", None])
def test_log_with_different_verbosity(
    tracker: ModificationTracker,
    demo_instance: UserDemo,
    verbosity: MODIFICATION_TRACKER_VERBOSITY,
    caplog,
):
    """Test logging with different verbosity levels."""
    tracker.verbosity = verbosity
    tracker.track_modification(demo_instance)

    with caplog.at_level(logging.INFO):
        tracker.log()

    if verbosity:
        assert len(caplog.records) > 0
        assert "Object modified locally but not committed" in caplog.records[0].message
        if verbosity == "INFO":
            assert any(
                "Full stack trace" in record.message for record in caplog.records
            )
    else:
        assert len(caplog.records) == 0


def test_multiple_model_types(tracker: ModificationTracker):
    """Test tracking modifications for different model types."""
    instance1 = UserDemo(id=1, name="test", email="test@example.com")
    instance2 = ArtifactDemo(id=2, title="test", user_id=1)

    tracker.track_modification(instance1)
    tracker.track_modification(instance2)

    assert len(tracker.modified_models) == 2
    assert id(instance1) in tracker.modified_models
    assert id(instance2) in tracker.modified_models


def test_clear_status_cleanup(tracker: ModificationTracker):
    """Test that clear_status properly cleans up empty model lists."""
    instance = UserDemo(id=1, name="test", email="test@example.com")
    tracker.track_modification(instance)

    assert id(instance) in tracker.modified_models
    tracker.clear_status([instance])
    assert id(instance) not in tracker.modified_models


def test_callback_registration(tracker: ModificationTracker):
    """
    Test that registering the tracker as a callback on a model instance
    properly tracks modifications when the model is changed.
    """
    instance = UserDemo(id=1, name="test", email="test@example.com")
    instance.register_modified_callback(tracker.track_modification)

    # Initially no modifications
    assert id(instance) not in tracker.modified_models

    # Modify the instance
    instance.name = "new name"

    # Should have tracked the modification
    assert id(instance) in tracker.modified_models
    modification = tracker.modified_models[id(instance)]
    assert modification.instance == instance
    assert "test_modifications.py" in modification.user_stack_trace

    # Another modification shouldn't create a new entry
    instance.email = "new@example.com"
    assert len(tracker.modified_models) == 1
