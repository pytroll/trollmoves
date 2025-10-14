"""Test Trollmoves base."""


def test_create_publisher():
    """Test that publisher is created"""
    from trollmoves.move_it_base import create_publisher

    pub = create_publisher(40000, "publisher_name")
    assert pub.name == "publisher_name"
    assert pub.port_number == 40000
