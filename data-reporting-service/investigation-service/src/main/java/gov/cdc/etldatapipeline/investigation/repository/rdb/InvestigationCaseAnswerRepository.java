package gov.cdc.etldatapipeline.investigation.repository.rdb;

import gov.cdc.etldatapipeline.investigation.repository.model.dto.InvestigationCaseAnswer;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface InvestigationCaseAnswerRepository extends JpaRepository<InvestigationCaseAnswer, Long> {
    List<InvestigationCaseAnswer> findByActUid(String actUid);

    void deleteAllByActUid(String actUid);

    void deleteByActUid(String actUid);

    void delete();
}
