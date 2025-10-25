import argparse
import sys

from pydantic import BaseModel, Field
from typing import Generator, Type, Union
from datetime import datetime, timedelta
from typing import Optional
import random
import csv
from faker import Faker
import logging

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

class BaseAuditRecord(BaseModel):
    audit_operation: str
    audit_timestamp: datetime
    schema: str = Field(default='public')
    table: str


class Order(BaseAuditRecord):
    table: str = 'orders'
    order_id: int
    order_timestamp: datetime
    created_at: datetime
    updated_at: datetime
    sum: float = Field(..., ge=0)
    description: Optional[str]


ObjTypes = Union[Order]
fake = Faker()

objects_map = {
    'order': Order
}


DEFAULT_MIN_RECORDS_PER_DAY_PER_DAY = 7
DEFAULT_MAX_RECORDS_PER_DAY_PER_DAY = 15
DEFAULT_WEEKS_BEFORE = 8
DEFAULT_WEEKS_AFTER = 52


parser = argparse.ArgumentParser()
parser.add_argument(
    "--min-records-per-day",
    type=int,
    default=DEFAULT_MIN_RECORDS_PER_DAY_PER_DAY,
    help="Minimum number of records to per day to generate"
)
parser.add_argument(
    "--max-records-per-day",
    type=int,
    default=DEFAULT_MAX_RECORDS_PER_DAY_PER_DAY,
    help="Maximum number of records to per day to generate"
)
parser.add_argument(
    "--weeks-before",
    type=int,
    default=DEFAULT_WEEKS_BEFORE,
    help="Number of weeks prior to current date to start generating data from"
)
parser.add_argument(
    "--weeks-after",
    type=int,
    default=DEFAULT_WEEKS_AFTER,
    help="Number of weeks after current date to stop generating data at"
)
parser.add_argument(
    "--obj-type",
    type=str,
    choices=objects_map.keys(),
    help="object types to generate"
)
parser.add_argument(
    "--output-file",
    type=str,
    default="./data/orders.csv",
    help="Output CSV file path"
)


def orders_generator(
        min_records_per_day: int,
        max_records_per_day: int,
        weeks_before: int,
        weeks_after: int
) -> Generator[list[Order], None, None]:
    interval_start = datetime.now() - timedelta(weeks=weeks_before)
    interval_end = datetime.now() + timedelta(weeks=weeks_after)
    num_days = (interval_end - interval_start).days
    interval_start_date = interval_start.date()
    dates = [(interval_start_date + timedelta(days=n), interval_start_date + timedelta(days=n+1)) for n in range(num_days)]

    for start_date, end_date in dates:

        num_records_per_day = random.randint(min_records_per_day, max_records_per_day)
        daily_data = []
        for i in range(1, num_records_per_day + 1):
            order_timestamp = fake.date_time_between(start_date=start_date, end_date=end_date)
            created_at = order_timestamp
            daily_data.append(
                Order(
                    audit_operation='I',
                    audit_timestamp=created_at + timedelta(milliseconds=random.randint(1, 300)),
                    order_id=i,
                    order_timestamp=order_timestamp,
                    created_at=created_at,
                    updated_at=created_at,
                    sum=round(random.uniform(10, 1000), 2),
                    description=fake.sentence(nb_words=6)
                )
            )

        yield daily_data


def generate_updates(records: list[BaseModel]) -> list[Order]:
    """Pick 3 random records and generate update records for them."""
    if len(records) < 3:
        raise ValueError("Not enough records to generate updates.")
    selected = random.sample(records, 3)
    updates = []
    if isinstance(selected[0], Order):
        for r in selected:
            updated_at = r.created_at + timedelta(hours=random.randint(1, 32))
            audit_timestamp = updated_at + timedelta(milliseconds=random.randint(1, 300))
            updated_record = Order(**r.model_dump())
            updated_record.audit_operation = 'U'
            updated_record.audit_timestamp = audit_timestamp
            updated_record.updated_at = updated_at
            updated_record.sum = round(random.uniform(10, 1000), 2)
            updated_record.description = fake.sentence(nb_words=8) + "(updated)"

            updates.append(updated_record)
    return updates

def generate_deletes(records: list[BaseModel]) -> list[BaseModel]:
    """Pick 3 random records and generate delete records for them."""
    if len(records) < 3:
        raise ValueError("Not enough records to generate deletes.")
    selected = random.sample(records, 3)
    deletes = []
    if isinstance(selected[0], Order):
        for r in selected:
            deleted_record = Order(**r.model_dump())
            deleted_record.audit_operation = 'D'
            deleted_record.audit_timestamp = r.updated_at + timedelta(hours=random.randint(1, 72))
            deletes.append(deleted_record)
    return deletes


def generate_records(
        min_records_per_day: int,
        max_records_per_day: int,
        weeks_before: int,
        weeks_after: int,
        obj_cls: ObjTypes
) -> Generator[list[BaseModel], None, None]:
    if obj_cls == Order:
        return orders_generator(
            min_records_per_day=min_records_per_day,
            max_records_per_day=max_records_per_day,
            weeks_before=weeks_before,
            weeks_after=weeks_after,
        )


def generate(
        min_records_per_day: int,
        max_records_per_day: int,
        weeks_before: int,
        weeks_after: int,
        obj_type: str,
        output_file: str
) -> None:
    obj_cls = objects_map.get(obj_type)
    if not obj_cls:
        raise ValueError(f"Unsupported object type: {obj_type}")

    records = list(
        generate_records(
            min_records_per_day=min_records_per_day,
            max_records_per_day=max_records_per_day,
            weeks_before=weeks_before,
            weeks_after=weeks_after,
            obj_cls=obj_cls
        )
    )
    all_records = []
    for i, daily_records in enumerate(records, start=1):
        update_records = generate_updates(daily_records)
        delete_records = generate_deletes(daily_records)
        all_daily_records = daily_records + update_records + delete_records

        logger.info(
            f"day={i}, generated {len(all_daily_records)} records for a day, "
            f"updates: {len(update_records)}, "
            f"deletes: {len(delete_records)}"
        )
        all_records.extend(all_daily_records)


    sorted_records = sorted(all_records, key=lambda r: r.audit_timestamp)
    save(sorted_records, output_file, header=obj_cls.model_fields.keys())


def save(records: Generator[BaseModel, None, None] | list, output_file: str, header: list[str]):
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for r in records:
            writer.writerow(r.model_dump())


if __name__ == '__main__':
    args = parser.parse_args()
    min_records_per_day = args.min_records_per_day
    max_records_per_day = args.max_records_per_day
    weeks_before = args.weeks_before
    weeks_after = args.weeks_after
    obj_type = args.obj_type
    output_file = args.output_file


    logger.info(f'Generating {min_records_per_day}, {max_records_per_day} records to {output_file} of type {obj_type}')
    generate(
        min_records_per_day,
        max_records_per_day,
        weeks_before,
        weeks_after,
        obj_type=obj_type,
        output_file=output_file
    )
    logger.info(f'Data generation completed and saved to {args.output_file}')