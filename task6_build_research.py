import os, sys, time
sys.path.insert(0, "src"); sys.path.insert(0, "benchmarks")
import osn_sample; osn_sample.load_dotenv()
spark = osn_sample.build_spark(4, "16g", distributed=False)
spark.sparkContext.setLogLevel("ERROR")
from opdi.config import OPDIConfig
from opdi.reference.h3_airport_layouts import AirportLayoutGenerator
from opdi.utils.storage import StorageManager

# Redirect the write to a research name for this first build.
orig = StorageManager._s3_path
StorageManager._s3_path = lambda self, t: orig(
    self, "research/hexaero_airport_layouts_pbf" if t == "hexaero_airport_layouts" else t
)
gen = AirportLayoutGenerator(spark, OPDIConfig.for_environment("opensky"),
                             log_dir="OPDI_live/logs/pbf",
                             pbf_path=os.environ["OPDI_OSM_PBF"])
t0 = time.time()
ok, bad = gen.process_all()
print("built %d, failed %d, %.0f s" % (len(ok), len(bad), time.time() - t0))
print("FAILED_ICAOS:", bad)
try:
    print("ASSIGNMENT_REPORT_HEAD")
    rep = gen._pbf_source.assignment_report()
    print(rep["method"].value_counts().to_string())
    rep.to_csv("task6_assignment_report.csv", index=False)
except Exception as e:
    print("assignment_report failed:", e)
print("TASK6_BUILD_DONE")
