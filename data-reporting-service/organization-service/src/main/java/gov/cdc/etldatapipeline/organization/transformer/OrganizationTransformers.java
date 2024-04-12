package gov.cdc.etldatapipeline.organization.transformer;

import gov.cdc.etldatapipeline.organization.model.DataRequiredFields;
import gov.cdc.etldatapipeline.organization.model.avro.DataEnvelope;
import gov.cdc.etldatapipeline.organization.model.dto.org.OrganizationElasticSearch;
import gov.cdc.etldatapipeline.organization.model.dto.org.OrganizationKey;
import gov.cdc.etldatapipeline.organization.model.dto.org.OrganizationReporting;
import gov.cdc.etldatapipeline.organization.model.dto.org.OrganizationSp;
import gov.cdc.etldatapipeline.organization.utils.UtilHelper;
import org.springframework.stereotype.Component;

@Component
public class OrganizationTransformers {
    UtilHelper utilHelper = new UtilHelper();

    public DataEnvelope buildOrganizationKey(OrganizationSp p) {
        return utilHelper.buildAvroRecord(OrganizationKey.builder().orgUID(p.getOrganizationUid()).build());
    }

    public DataEnvelope processData(OrganizationSp organizationSp, OrganizationType organizationType) {
        DataRequiredFields transformedObj =
                switch (organizationType) {
                    case ORGANIZATION_REPORTING -> buildOrganizationReporting(organizationSp);
                    case ORGANIZATION_ELASTIC_SEARCH -> buildOrganizationElasticSearch(organizationSp);
                };
        DataPostProcessor processor = new DataPostProcessor();
        processor.processOrgAddress(organizationSp.getOrganizationAddress(), transformedObj);
        processor.processOrgPhone(organizationSp.getOrganizationTelephone(), transformedObj);
        processor.processOrgFax(organizationSp.getOrganizationFax(), transformedObj);
        processor.processOrgEntity(organizationSp.getOrganizationEntityId(), transformedObj);
        processor.processOrgName(organizationSp.getOrganizationName(), transformedObj);
        return utilHelper.buildAvroRecord(transformedObj);
    }

    public OrganizationElasticSearch buildOrganizationElasticSearch(OrganizationSp orgSp) {
        return OrganizationElasticSearch.builder()
                .organizationUid(orgSp.getOrganizationUid())
                .cd(orgSp.getCd())
                .statusCd(orgSp.getStatusCd())
                .statusTime(orgSp.getStatusTime())
                .versionCtrlNbr(orgSp.getVersionCtrlNbr())
                .edxInd(orgSp.getEdxInd())
                .recordStatusTime(orgSp.getRecordStatusTime())
                .localId(orgSp.getLocalId())
                .orgRecordStatusCd(orgSp.getRecordStatusCd())
                .description(orgSp.getDescription())
                .electronicInd(orgSp.getElectronicInd())
                .standIndClass(orgSp.getStandIndClass())
                .addUserId(orgSp.getAddUserId())
                .addTime(orgSp.getAddTime())
                .lastChgUserId(orgSp.getLastChgUserId())
                .lastChgTime(orgSp.getLastChgTime())
                .build();
    }

    public OrganizationReporting buildOrganizationReporting(OrganizationSp orgSp) {
        return OrganizationReporting.builder()
                .organizationUid(orgSp.getOrganizationUid())
                .localId(orgSp.getLocalId())
                .recordStatus(orgSp.getRecordStatusCd())
                .generalComments(orgSp.getDescription())
                .entryMethod(orgSp.getElectronicInd())
                .standIndClass(orgSp.getStandIndClass())
                .organizationName(orgSp.getOrganizationName())
                .addTime(orgSp.getAddTime())
                .addUserId(orgSp.getAddUserId())
                .lastChgUserId(orgSp.getLastChgUserId())
                .lastChgTime(orgSp.getLastChgTime())
                .addUserName(orgSp.getAddUserName())
                .lastChgUserName(orgSp.getLastChgUserName())
                .build();
    }
}
