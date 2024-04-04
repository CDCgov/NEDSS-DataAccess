package gov.cdc.etldatapipeline.organization.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.cdc.etldatapipeline.organization.model.dto.orgdetails.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.ObjectUtils;

import java.util.Arrays;
import java.util.Comparator;

@Slf4j
public class DataPostProcessor {
    ObjectMapper mapper = new ObjectMapper();

    public <T> void processAllProps(String name, String entity, String address, String phone, String fax, T org) {
        processOrgName(name, org);
        processOrgEntity(entity, org);
        processOrgAddress(address, org);
        processOgrPhone(phone, org);
        processOgrFax(fax, org);
    }

    public <T> void processOrgName(String name, T org) {
        if (!ObjectUtils.isEmpty(name)) {
            try {
                Arrays.stream(mapper.readValue(name, Name[].class))
                        .filter(oName -> !ObjectUtils.isEmpty(oName.getOnOrgUid()))
                        .max(Comparator.comparing(Name::getOnOrgUid))
                        .map(n -> n.updateOrg(org));
            } catch (JsonProcessingException e) {
                log.error("Processing exception : " + org + ". Exception + " + e);
            }
        }
    }

    public <T> void processOrgEntity(String entity, T org) {
        if (!ObjectUtils.isEmpty(entity)) {
            try {
                Arrays.stream(mapper.readValue(entity, Entity[].class))
                        .filter(oEntity -> !ObjectUtils.isEmpty(oEntity.getEntityIdSeq()))
                        .max(Comparator.comparing(Entity::getEntityIdSeq))
                        .map(n -> n.updateOrg(org));
            } catch (JsonProcessingException e) {
                log.error("Processing exception : " + org + ". Exception + " + e);
            }
        }
    }

    public <T> void processOrgAddress(String address, T org) {
        if (!ObjectUtils.isEmpty(address)) {
            try {
                Arrays.stream(mapper.readValue(address, Address[].class))
                        .filter(oAddr -> !ObjectUtils.isEmpty(oAddr.getAddrPlUid()))
                        .max(Comparator.comparing(Address::getAddrPlUid))
                        .map(n -> n.updateOrg(org));
            } catch (JsonProcessingException e) {
                log.error("Processing exception : " + org + ". Exception + " + e);
            }
        }
    }

    public <T> void processOgrPhone(String phone, T org) {
        if (!ObjectUtils.isEmpty(phone)) {
            try {
                Arrays.stream(mapper.readValue(phone, Phone[].class))
                        .filter(oPhone -> !ObjectUtils.isEmpty(oPhone.getPhTlUid()))
                        .max(Comparator.comparing(Phone::getPhTlUid))
                        .map(n -> n.updateOrg(org));
            } catch (JsonProcessingException e) {
                log.error("Processing exception : " + org + ". Exception + " + e);
            }
        }
    }

    public <T> void processOgrFax(String fax, T org) {
        try {
            if (!ObjectUtils.isEmpty(fax)) {
                Arrays.stream(mapper.readValue(fax, Fax[].class))
                        .filter(oPhone -> !ObjectUtils.isEmpty(oPhone.getFaxTlUid()))
                        .max(Comparator.comparing(Fax::getFaxTlUid))
                        .map(n -> n.updateOrg(org));
            }
        } catch (JsonProcessingException e) {
            log.error("Processing exception : " + org + ". Exception + " + e);
        }
    }
}
