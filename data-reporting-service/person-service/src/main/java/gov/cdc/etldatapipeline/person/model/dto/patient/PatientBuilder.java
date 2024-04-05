package gov.cdc.etldatapipeline.person.model.dto.patient;

public interface PatientBuilder {
    <U> U constructObject(Patient p);
}
