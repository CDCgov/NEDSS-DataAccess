package gov.cdc.etldatapipeline.changedata.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.cdc.etldatapipeline.changedata.model.dto.PersonExtendedProps;
import gov.cdc.etldatapipeline.changedata.model.dto.persondetail.*;
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
public class DataPostProcessor {
    ObjectMapper mapper = new ObjectMapper();

    public <T extends PersonExtendedProps> void processPersonName(String name, T pf) throws JsonProcessingException {
        if (!ObjectUtils.isEmpty(name)) {
            Arrays.stream(mapper.readValue(name, Name[].class))
                    .max(Comparator.comparing(Name::getPersonUid))
                    .map(n -> n.updatePerson(pf));
        }
    }

    public <T extends PersonExtendedProps> void  processPersonAddress(String address, T pf) throws JsonProcessingException {
        if (!ObjectUtils.isEmpty(address)) {
            Arrays.stream(mapper.readValue(address, Address[].class))
                    .max(Comparator.comparing(Address::getPostalLocatorUid))
                    .map(n -> n.updatePerson(pf));
        }
    }

    public <T extends PersonExtendedProps> void  processPersonRace(String race, T pf) throws JsonProcessingException {
        if (!ObjectUtils.isEmpty(race)) {
            Arrays.stream(mapper.readValue(race, Race[].class))
                    .max(Comparator.comparing(Race::getPersonUid))
                    .map(n -> n.updatePerson(pf));
        }
    }

    public <T extends PersonExtendedProps> void  processPersonTelephone(String telephone, T pf) throws JsonProcessingException {
        if (!ObjectUtils.isEmpty(telephone)) {
            Function<String, T> personPhoneFn =
                    (useCd) -> Arrays.stream(
                                    UtilHelper.getInstance().deserializePayload(telephone, Phone[].class))
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

    public <T extends PersonExtendedProps> void  processPersonAddAuth(String addAuthNested, T pf) throws JsonProcessingException {
        if (!ObjectUtils.isEmpty(addAuthNested)) {
            Arrays.stream(mapper.readValue(addAuthNested, AddAuthUser[].class))
                    .max(Comparator.comparing(AddAuthUser::getAddUserChgTime))
                    .map(n -> n.updatePerson(pf));
        }
    }

    public <T extends PersonExtendedProps> void  processPersonChangeAuth(String chgAuthNested, T pf) throws JsonProcessingException {
        if (!ObjectUtils.isEmpty(chgAuthNested)) {
            Arrays.stream(mapper.readValue(chgAuthNested, ChgAuthUser[].class))
                    .max(Comparator.comparing(ChgAuthUser::getLastChgUserTime))
                    .map(n -> n.updatePerson(pf));
        }
    }

    public <T extends PersonExtendedProps> void  processPersonEntityData(String entityData, T pf) throws JsonProcessingException {
        if (!ObjectUtils.isEmpty(entityData)) {
            Function<Predicate<? super EntityData>, T> entityDataTypeCdFn =
                    (Predicate<? super EntityData> p) -> Arrays.stream(
                                    UtilHelper.getInstance().deserializePayload(entityData, EntityData[].class))
                            .filter(p)
                            .filter(e -> e.getEntityIdSeq() != null)
                            .max(Comparator.comparing(EntityData::getEntityIdSeq))
                            .map(n -> n.updatePerson(pf))
                            .orElse(null);
            entityDataTypeCdFn.apply(e -> StringUtils.hasText(e.getAssigningAuthorityCd())
                    && e.getAssigningAuthorityCd().equalsIgnoreCase("SSA"));
            entityDataTypeCdFn.apply(e -> StringUtils.hasText(e.getTypeCd())
                    && e.getTypeCd().equalsIgnoreCase("PN"));
            entityDataTypeCdFn.apply(e ->  StringUtils.hasText(e.getTypeCd())
                    && e.getTypeCd().equalsIgnoreCase("QEC"));
            entityDataTypeCdFn.apply(e ->  StringUtils.hasText(e.getTypeCd())
                    && e.getTypeCd().equalsIgnoreCase("PRN"));
        }
    }

    public <T extends PersonExtendedProps> void processPersonEmail(String email, T pf) throws JsonProcessingException {
        if (!ObjectUtils.isEmpty(email)) {
            Arrays.stream(mapper.readValue(email, Email[].class))
                    .max(Comparator.comparing(Email::getTeleLocatorUid))
                    .map(n -> n.updatePerson(pf));
        }
    }
}
