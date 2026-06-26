import pytest
from unittest.mock import MagicMock
from sbn_parsl.validation import (
    parse_walltime_to_seconds,
    check_queue_bounds,
    validate_queue_limits,
)

def test_parse_walltime_to_seconds():
    assert parse_walltime_to_seconds("01:00:00") == 3600
    assert parse_walltime_to_seconds("00:05:00") == 300
    assert parse_walltime_to_seconds("1-12:00:00") == 86400 + 43200
    assert parse_walltime_to_seconds("2:30") == 150
    assert parse_walltime_to_seconds("45") == 45
    
    with pytest.raises(ValueError):
        parse_walltime_to_seconds("invalid")
        
    with pytest.raises(ValueError):
        parse_walltime_to_seconds("1:2:3:4")

def test_check_queue_bounds():
    q_limits = {
        "nodes_min": 10,
        "nodes_max": 24,
        "walltime_min": "00:05:00",
        "walltime_max": "03:00:00"
    }
    
    # Valid
    ok, err = check_queue_bounds(15, "01:00:00", "small", q_limits)
    assert ok
    assert err == ""
    
    # Node too low
    ok, err = check_queue_bounds(5, "01:00:00", "small", q_limits)
    assert not ok
    assert "nodes=5" in err
    
    # Node too high
    ok, err = check_queue_bounds(30, "01:00:00", "small", q_limits)
    assert not ok
    assert "nodes=30" in err
    
    # Walltime too low
    ok, err = check_queue_bounds(15, "00:01:00", "small", q_limits)
    assert not ok
    assert "walltime=00:01:00" in err
    
    # Walltime too high
    ok, err = check_queue_bounds(15, "04:00:00", "small", q_limits)
    assert not ok
    assert "walltime=04:00:00" in err

def test_validate_queue_limits(capsys):
    # Setup mock Config
    cfg = MagicMock()
    cfg.site.name = "polaris"
    cfg.site.queues = {
        "debug": {
            "nodes_min": 1,
            "nodes_max": 2,
            "walltime_min": "00:05:00",
            "walltime_max": "01:00:00"
        },
        "prod": {
            "is_routing": True,
            "routes_to": ["small", "medium"]
        },
        "small": {
            "nodes_min": 10,
            "nodes_max": 24,
            "walltime_min": "00:05:00",
            "walltime_max": "03:00:00"
        },
        "medium": {
            "nodes_min": 25,
            "nodes_max": 99,
            "walltime_min": "00:05:00",
            "walltime_max": "06:00:00"
        }
    }
    
    # 1. Valid non-routing
    cfg.job.queue = "debug"
    cfg.job.nodes_per_block = 2
    cfg.job.walltime = "00:30:00"
    
    validate_queue_limits(cfg)
    captured = capsys.readouterr()
    assert captured.err == ""
    
    # 2. Invalid non-routing
    cfg.job.nodes_per_block = 5
    with pytest.raises(ValueError) as excinfo:
        validate_queue_limits(cfg)
    assert "Job queue validation failed for queue 'debug'" in str(excinfo.value)

    # Verify warning is printed when force=True
    validate_queue_limits(cfg, force=True)
    captured = capsys.readouterr()
    assert "WARNING: Job queue validation failed for queue 'debug'" in captured.err
    
    # 3. Valid routing
    cfg.job.queue = "prod"
    cfg.job.nodes_per_block = 15
    cfg.job.walltime = "02:00:00"
    
    validate_queue_limits(cfg)
    captured = capsys.readouterr()
    assert captured.err == ""
    
    # 4. Invalid routing (doesn't fit either small or medium)
    cfg.job.nodes_per_block = 5  # Too small for routing queues
    with pytest.raises(ValueError) as excinfo:
        validate_queue_limits(cfg)
    assert "do not match any execution sub-queues for routing queue 'prod'" in str(excinfo.value)

    # Verify warning is printed when force=True
    validate_queue_limits(cfg, force=True)
    captured = capsys.readouterr()
    assert "do not match any execution sub-queues for routing queue 'prod'" in captured.err
    
    # 5. Unknown queue on non-local site (should warn)
    cfg.job.queue = "unknown"
    validate_queue_limits(cfg)
    captured = capsys.readouterr()
    assert "WARNING: No known queue limits for site 'polaris' queue 'unknown'" in captured.err
    
    # 6. Unknown queue on local site (should not warn)
    cfg.site.name = "local"
    cfg.job.queue = "unknown"
    validate_queue_limits(cfg)
    captured = capsys.readouterr()
    assert captured.err == ""
