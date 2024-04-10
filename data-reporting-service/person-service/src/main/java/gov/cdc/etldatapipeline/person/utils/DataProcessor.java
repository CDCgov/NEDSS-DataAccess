package gov.cdc.etldatapipeline.person.utils;

import gov.cdc.etldatapipeline.person.model.dto.PersonExtendedProps;
import gov.cdc.etldatapipeline.person.model.dto.patient.PatientSp;
import gov.cdc.etldatapipeline.person.model.dto.persondetail.*;
import gov.cdc.etldatapipeline.person.model.dto.provider.ProviderSp;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.Comparator;
import java.util.function.Function;
import java.util.function.Predicate;

@Slf4j
@NoArgsConstructor
public class DataProcessor {
    UtilHelper utilHelper = UtilHelper.getInstance();

    /**
     * Transforms the nested data elements in PatientSp to the individual properties
     *
     * @param patientObj PatientReporting/PatientElasticSearch object
     * @param <T>        Any object extending PersonExtendedProps
     * @return Transformed object
     */
    public static <T extends PersonExtendedProps> T processPatientData(PatientSp patientSp, T patientObj) {
        DataProcessor processor = new DataProcessor();
        processor.processPersonName(patientSp.getNameNested(), patientObj);
        processor.processPersonAddress(patientSp.getAddressNested(), patientObj);
        processor.processPersonTelephone(patientSp.getTelephoneNested(), patientObj);
        processor.processPersonEntityData(patientSp.getEntityDataNested(), patientObj);
        processor.processPersonEmail(patientSp.getEmailNested(), patientObj);
        processor.processPersonRace(patientSp.getRaceNested(), patientObj);
        return patientObj;
    }

    public static <T extends PersonExtendedProps> T processProviderData(ProviderSp patientSp, T patientObj) {
        DataProcessor processor = new DataProcessor();
        processor.processPersonName(patientSp.getNameNested(), patientObj);
        processor.processPersonAddress(patientSp.getAddressNested(), patientObj);
        processor.processPersonTelephone(patientSp.getTelephoneNested(), patientObj);
        processor.processPersonEntityData(patientSp.getEntityDataNested(), patientObj);
        processor.processPersonEmail(patientSp.getEmailNested(), patientObj);
        return patientObj;
    }

    public <T extends PersonExtendedProps> void processPersonName(String name, T pf) {
        if (!ObjectUtils.isEmpty(name)) {
            Arrays.stream(utilHelper.deserializePayload(name, Name[].class))
                    .filter(pName -> !ObjectUtils.isEmpty(pName.getPersonUid()))
                    .max(Comparator.comparing(Name::getPersonUid))
                    .map(n -> n.updatePerson(pf));
        }
    }

    public <T extends PersonExtendedProps> void processPersonAddress(String address, T pf) {
        if (!ObjectUtils.isEmpty(address)) {
            Arrays.stream(utilHelper.deserializePayload(address, Address[].class))
                    .filter(pAddress -> !ObjectUtils.isEmpty(pAddress.getPostalLocatorUid()))
                    .max(Comparator.comparing(Address::getPostalLocatorUid))
                    .map(n -> n.updatePerson(pf));
        }
    }

    public <T extends PersonExtendedProps> void processPersonRace(String race, T pf) {
        if (!ObjectUtils.isEmpty(race)) {
            Arrays.stream(utilHelper.deserializePayload(race, Race[].class))
                    .filter(pRace -> !ObjectUtils.isEmpty(pRace.getPersonUid()))
                    .max(Comparator.comparing(Race::getPersonUid))
                    .map(n -> n.updatePerson(pf));
        }
    }

    public <T extends PersonExtendedProps> void processPersonTelephone(String telephone, T pf) {
        if (!ObjectUtils.isEmpty(telephone)) {
            Function<String, T> personPhoneFn =
                    (useCd) -> Arrays.stream(utilHelper.deserializePayload(telephone, Phone[].class))
                            .filter(phone -> StringUtils.hasText(phone.getUseCd())
                                    && phone.getUseCd().equalsIgnoreCase(useCd))
                            .max(Comparator.comparing(Phone::getTeleLocatorUid))
                            .map(n -> n.updatePerson(pf))
                            .orElse(null);
            personPhoneFn.apply("WP");
            personPhoneFn.apply("H");
            personPhoneFn.apply("CP");
        }
    }

    public <T extends PersonExtendedProps> void processPersonEntityData(String entityData, T pf) {
        if (!ObjectUtils.isEmpty(entityData)) {
            Function<Predicate<? super EntityData>, T> entityDataTypeCdFn =
                    (Predicate<? super EntityData> p) -> Arrays.stream(
                                    utilHelper.deserializePayload(entityData, EntityData[].class))
                            .filter(p)
                            .filter(e -> !ObjectUtils.isEmpty(e.getEntityIdSeq()))
                            .max(Comparator.comparing(EntityData::getEntityIdSeq))
                            .map(n -> n.updatePerson(pf))
                            .orElse(null);
            entityDataTypeCdFn.apply(e -> StringUtils.hasText(e.getAssigningAuthorityCd())
                    && e.getAssigningAuthorityCd().equalsIgnoreCase("SSA"));
            entityDataTypeCdFn.apply(e -> StringUtils.hasText(e.getTypeCd())
                    && e.getTypeCd().equalsIgnoreCase("PN"));
            entityDataTypeCdFn.apply(e -> StringUtils.hasText(e.getTypeCd())
                    && e.getTypeCd().equalsIgnoreCase("QEC"));
            entityDataTypeCdFn.apply(e -> StringUtils.hasText(e.getTypeCd())
                    && e.getTypeCd().equalsIgnoreCase("PRN"));
        }
    }

    public <T extends PersonExtendedProps> void processPersonEmail(String email, T pf) {
        if (!ObjectUtils.isEmpty(email)) {
            Arrays.stream(utilHelper.deserializePayload(email, Email[].class))
                    .filter(pEmail -> !ObjectUtils.isEmpty(pEmail.getTeleLocatorUid()))
                    .max(Comparator.comparing(Email::getTeleLocatorUid))
                    .map(n -> n.updatePerson(pf));
        }
    }
}
