package gov.cdc.etldatapipeline.investigation.repository.model.dto;


import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@ToString
@Getter
@Setter
@RequiredArgsConstructor
public class DummyClass {
    private Long id;
    private String firstName;
    private String lastName;
    private String email;

    public DummyClass(long l, String firstName, String lastName, String mail) {
        this.id = l;
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = mail;
    }
}
