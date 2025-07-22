import factory
from datetime import timezone
from faker import Faker
from app.models.data_source import DataSource
from app.schemas.data_source import DataSourceCreateRequest, DataSourceUpdateRequest
from app.schemas.enum import DataSourceType


fake = Faker()


class DataSourceFactory(factory.Factory):
    """Factory for creating DataSource test objects."""
    
    class Meta:
        model = DataSource
    
    data_source_id = factory.Sequence(lambda n: n)
    data_source_user_id = factory.Faker('random_int', min=1, max=1000)
    data_source_name = factory.Faker('company')
    data_source_type = factory.Faker(
        'random_element', 
        elements=[ds_type for ds_type in DataSourceType]
    )
    data_source_url = factory.LazyAttribute(
        lambda obj: DataSourceFactory._generate_url_for_type(obj.data_source_type)
    )
    data_source_created_at = factory.Faker(
        'date_time_between', 
        start_date='-30d', 
        end_date='now',
        tzinfo=timezone.utc
    )
    data_source_updated_at = factory.LazyAttribute(
        lambda obj: obj.data_source_created_at
    )
    
    @staticmethod
    def _generate_url_for_type(data_source_type: DataSourceType) -> str:
        """Generate appropriate URL based on data source type."""
        if data_source_type == DataSourceType.CSV:
            return fake.url() + "/data.csv"
        elif data_source_type == DataSourceType.XLSX:
            return fake.url() + "/data.xlsx"
        elif data_source_type == DataSourceType.PDF:
            return fake.url() + "/document.pdf"
        elif data_source_type == DataSourceType.GOOGLE:
            return "https://docs.google.com/spreadsheets/d/" + fake.lexify(text='?' * 44)
        elif data_source_type == DataSourceType.POSTGRES:
            return f"postgresql://{fake.user_name()}:{fake.password()}@{fake.hostname()}:5432/{fake.word()}"
        elif data_source_type == DataSourceType.MYSQL:
            return f"mysql://{fake.user_name()}:{fake.password()}@{fake.hostname()}:3306/{fake.word()}"
        elif data_source_type == DataSourceType.MSSQL:
            return f"Server={fake.hostname()};Database={fake.word()};Trusted_Connection=yes;"
        elif data_source_type == DataSourceType.ORACLE:
            return f"{fake.hostname()}:1521:{fake.word().upper()}"
        elif data_source_type == DataSourceType.NOSQL:
            return f"mongodb://{fake.user_name()}:{fake.password()}@{fake.hostname()}:27017/{fake.word()}"
        else:
            return fake.url()


class DataSourceCreateRequestFactory(factory.Factory):
    """Factory for creating DataSourceCreateRequest test objects."""
    
    class Meta:
        model = DataSourceCreateRequest
    
    data_source_name = factory.Faker('company')
    data_source_type = factory.Faker(
        'random_element', 
        elements=[ds_type for ds_type in DataSourceType]
    )
    data_source_url = factory.LazyAttribute(
        lambda obj: DataSourceFactory._generate_url_for_type(obj.data_source_type)
    )


class DataSourceUpdateRequestFactory(factory.Factory):
    """Factory for creating DataSourceUpdateRequest test objects."""
    
    class Meta:
        model = DataSourceUpdateRequest
    
    data_source_name = factory.Faker('company')
    data_source_type = factory.Faker(
        'random_element', 
        elements=[ds_type for ds_type in DataSourceType]
    )
    data_source_url = factory.LazyAttribute(
        lambda obj: DataSourceFactory._generate_url_for_type(obj.data_source_type) 
        if obj.data_source_type else None
    )