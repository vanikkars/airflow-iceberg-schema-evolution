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


DEFAULT_NUM_RECORDS = 1000


parser = argparse.ArgumentParser()
parser.add_argument(
    "--num-records",
    type=int,
    default=DEFAULT_NUM_RECORDS,
    help="Number of records to generate"
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


def orders_generator(num_records) -> Generator[Order, None, None]:
    for i in range(1, num_records + 1):
        order_timestamp = fake.date_time_between(start_date='-3w', end_date='+3w')
        created_at = order_timestamp
        yield Order(
            audit_operation='I',
            audit_timestamp=created_at + timedelta(milliseconds=random.randint(1, 300)),
            order_id=i,
            order_timestamp=order_timestamp,
            created_at=created_at,
            updated_at=created_at,
            sum=round(random.uniform(10, 1000), 2),
            description=fake.sentence(nb_words=6)
        )

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


def generate_records(num_records: int, obj_cls: ObjTypes) -> Generator[BaseModel, None, None]:
    if obj_cls == Order:
        return orders_generator(num_records)


def generate(num_records: int, obj_type: str, output_file: str) -> None:
    obj_cls = objects_map.get(obj_type)
    if not obj_cls:
        raise ValueError(f"Unsupported object type: {obj_type}")

    records = list(generate_records(num_records, obj_cls))
    records_with_updates = records + generate_updates(records) + generate_deletes(records)
    sorted_records = sorted(records_with_updates, key=lambda r: r.audit_timestamp)
    save(sorted_records, output_file, header=obj_cls.model_fields.keys())


def save(records: Generator[BaseModel, None, None] | list, output_file: str, header: list[str]):
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for r in records:
            writer.writerow(r.model_dump())


if __name__ == '__main__':
    args = parser.parse_args()
    num_records = args.num_records
    obj_type = args.obj_type
    output_file = args.output_file


    logger.info(f'Generating {num_records} records to {output_file} of type {obj_type}')
    generate(
        num_records,
        obj_type=obj_type,
        output_file=output_file
    )
    logger.info(f'Data generation completed and saved to {args.output_file}')