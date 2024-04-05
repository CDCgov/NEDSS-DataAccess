package gov.cdc.etldatapipeline.person.model.dto.provider;

public interface ProviderBuilder {
    <U> U constructObject(Provider p);
}
