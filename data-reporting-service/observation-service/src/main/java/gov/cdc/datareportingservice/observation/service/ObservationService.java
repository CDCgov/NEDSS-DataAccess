package gov.cdc.datareportingservice.observation.service;


import gov.cdc.datareportingservice.observation.repository.IObservationRepository;
import gov.cdc.datareportingservice.observation.repository.model.Observation;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class ObservationService {
    private final IObservationRepository iObservationRepository;

    public ObservationService(IObservationRepository iObservationRepository) {
        this.iObservationRepository = iObservationRepository;
    }

    public void processObservationIds(String message) {
        Optional<Observation> observationData = iObservationRepository.findAllBy(message);
        System.out.println("observationData is..." + observationData);
    }
}
