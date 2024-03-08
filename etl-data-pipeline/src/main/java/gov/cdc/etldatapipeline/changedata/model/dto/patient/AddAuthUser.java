package gov.cdc.etldatapipeline.changedata.model.dto.patient;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import gov.cdc.etldatapipeline.changedata.model.dto.PatientOP;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class AddAuthUser {
    @JsonProperty("ADD_USER_ID")
    private Long addUserId;
    @JsonProperty("add_time")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS", timezone = "UTC")
    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    private LocalDateTime addTime;
    @JsonProperty("addUserChgTime")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS", timezone = "UTC")
    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    private LocalDateTime addUserChgTime;
    @JsonProperty("patientAddedBy")
    private String patientAddedBy;

    public PatientOP updatePerson(PatientOP patient) {
        patient.setPatientAddedBy(addUserId);
        return patient;
    }
}
