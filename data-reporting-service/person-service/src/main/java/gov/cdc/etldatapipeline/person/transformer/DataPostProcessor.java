package gov.cdc.etldatapipeline.person.transformer;

import gov.cdc.etldatapipeline.person.model.dto.PersonExtendedProps;
import gov.cdc.etldatapipeline.person.model.dto.persondetail.*;
import gov.cdc.etldatapipeline.person.utils.UtilHelper;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

@Slf4j
@NoArgsConstructor
public class DataPostProcessor {
    UtilHelper utilHelper = UtilHelper.getInstance();

    public <T extends PersonExtendedProps> void processPersonName(String name, T pf) {
        if (!ObjectUtils.isEmpty(name)) {
            List<Name> nameList = Arrays.stream(utilHelper.deserializePayload(name, Name[].class))
                    .filter(pName -> !ObjectUtils.isEmpty(pName.getPersonUid()))
                    .collect(groupingBy(Name::getPersonUid, TreeMap::new, toList()))
                    .lastEntry()
                    .getValue();
            if (nameList.size() == 1) {
                nameList.get(0).updatePerson(pf);
            } else if (nameList.size() > 1) {
                nameList.stream()
                        .filter(pName -> !ObjectUtils.isEmpty(pName.getPersonNmSeq()))
                        .max(Comparator.comparing(Name::getPersonNmSeq))
                        .map(n -> n.updatePerson(pf));
            }
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
