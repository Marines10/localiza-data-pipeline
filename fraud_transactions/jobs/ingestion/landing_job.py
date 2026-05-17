import shutil
from datetime import date

from fraud_transactions.utils.paths import SOURCE_FILE, LANDING_PATH


class LandingJob:

    def run(self) -> None:

        current_date = date.today().isoformat()

        sink_landing_path = LANDING_PATH / current_date
        sink_landing_path.mkdir(parents=True, exist_ok=True)

        landing_file = sink_landing_path / SOURCE_FILE.name

        shutil.copyfile(SOURCE_FILE, landing_file)

        print(f"Arquivo copiado para landing: {landing_file}")


if __name__ == "__main__":
    job = LandingJob()
    job.run()