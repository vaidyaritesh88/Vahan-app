"""Primary sales segment hierarchy, ordering, and grouping configuration."""

# ── PV (4W) Segment Configuration ──

PV_SEGMENT_ORDER = [
    # PC segments
    "Entry Hatchback", "Compact Hatchback", "Premium Hatchback",
    "Super Premium Hatchback", "Compact Sedan", "Upper Sedan", "Vans",
    # UV segments
    "Sub-compact SUV", "Compact SUV", "Mid-SUV", "Premium SUV", "MUV",
]

PV_SUPER_SEGMENTS = {
    "PC": ["Entry Hatchback", "Compact Hatchback", "Premium Hatchback",
           "Super Premium Hatchback", "Compact Sedan", "Upper Sedan", "Vans"],
    "UV": ["Sub-compact SUV", "Compact SUV", "Mid-SUV", "Premium SUV", "MUV"],
}

# Reverse lookup: segment -> super-segment
PV_SEGMENT_TO_SUPER = {}
for super_seg, segments in PV_SUPER_SEGMENTS.items():
    for seg in segments:
        PV_SEGMENT_TO_SUPER[seg] = super_seg


# ── 2W Segment Configuration ──

TW_SEGMENT_ORDER = [
    # Motorcycle sub-segments
    "Economy Segment", "Entry Executive", "Executive", "Premium",
    "Sports", "Sports Super Premium", "Classic Premium", "Classic Super Premium",
    # Other types
    "Scooter", "Moped", "EV",
]

TW_SUPER_SEGMENTS = {
    "Motorcycle": ["Economy Segment", "Entry Executive", "Executive", "Premium",
                   "Sports", "Sports Super Premium", "Classic Premium",
                   "Classic Super Premium"],
    "Scooter": ["Scooter"],
    "Moped": ["Moped"],
    "EV": ["EV"],
}

TW_SEGMENT_TO_SUPER = {}
for super_seg, segments in TW_SUPER_SEGMENTS.items():
    for seg in segments:
        TW_SEGMENT_TO_SUPER[seg] = super_seg


def get_segment_order(category):
    """Get ordered list of segments for a category."""
    if category == "PV":
        return PV_SEGMENT_ORDER
    return TW_SEGMENT_ORDER


def get_super_segments(category):
    """Get super-segment grouping for a category."""
    if category == "PV":
        return PV_SUPER_SEGMENTS
    return TW_SUPER_SEGMENTS


def get_segment_to_super(category):
    """Get segment -> super-segment mapping."""
    if category == "PV":
        return PV_SEGMENT_TO_SUPER
    return TW_SEGMENT_TO_SUPER


def get_super_segment_order(category):
    """Get ordered list of super-segments."""
    if category == "PV":
        return ["PC", "UV"]
    return ["Motorcycle", "Scooter", "Moped", "EV"]
