#!/miniconda-latest/bin/python
from collections import defaultdict
from dataclasses import dataclass

import htcondor
from htcondor import AdTypes


@dataclass
class InventoryItem:
    """Class for keeping track of an item in inventory."""

    name: str
    unit_price: float
    quantity_on_hand: int = 0


@dataclass
class Machine:
    # General Attributes
    name: str
    unit_price: float

    # # Resource Attributes
    # cpus: int
    # memory: int
    # disk: int

    # cpus_reserved: int
    # memory_reserved: int
    # disk_reserved: int

    # Condor Ads Mapping
    detected_cpus: int
    detected_memory_mb: int
    disk_kb: int
    remaining_memory_mb: int
    remaining_cpus: int
    dynamic_slot: int
    slot_type: int
    healthy: str
    partionable_slot: int
    dynamic_slot_num: int
    recent_job_starts: int
    state: str
    totalcpus: int
    totaldisk: int
    totalloadaverage: int
    totalmemory: int
    totalslotcpus: int

    # Condor Ads Mapping
    # Child attributes
    child_cpus: int
    child_disk_kb: int
    child_memory_mb: int
    child_names: str

    # Calculate these from jobs
    cpus_actual: int = None
    memory_actual: int = None
    disk_actual: int = None

    # Calculate these by hand
    cpus_unreserved: int = None
    memory_unreserved: int = None
    disk_unreserved: int = None


class Slots:
    name = "slot1"
    job_id = "jobid1"
    # request reserve actual
    # method name
    # object names?
    # object sizes?


collector = htcondor.Collector()
classads = collector.query(AdTypes.Startd)

machines = defaultdict(list)
for classad in classads:

    desired_attributes = [
        "Machine",
        "CLIENTGROUP",
        "ChildCpus",
        "ChildDisk" "ChildMemory",
        "ChildName",
        "DetectedCpus",
        "DetectedMemory",
        "Disk",
        "Memory",
        "Cpus",
        "DynamicSlot",
        "SlotType",
        "NODE_IS_HEALTHY",
        "PartitionableSlot",
        "NumDynamicSlots",
        "RecentJobStarts",
        "State",
        "TotalCpus",
        "TotalDisk",
        "TotalLoadAvg",
        "TotalMemory",
        "TotalSlotCpus",
    ]

    retrieved_ads = {}
    for attribute in desired_attributes:
        retrieved_ads[attribute] = classad.get(attribute)

    if retrieved_ads["SlotType"] == "Partitionable":
        retrieved_ads["InUse"] = True if retrieved_ads["NumDynamicSlots"] > 0 else False

    if "dtn" in classad["Machine"]:
        machines[classad["Machine"]].append(retrieved_ads)
        machines[classad["Machine"]].append(retrieved_ads)

    # "TotalSlotDisk": 39142918128.0,
    # "TotalSlotMemory": 257686,
    # "TotalSlots": 17,
    # "VirtualMemory": 263871408,

    # machines[classad['Machine']].append(m)
    #
    # machine = classad['Machine']
    # if machine == "uploadworker-prod-dtn2-clone":
    #     machines[machine].append(classad)

for machine in machines:
    print("The machine is")
    print(machine)
    print(machines[machine])

    print("=" * 50)
