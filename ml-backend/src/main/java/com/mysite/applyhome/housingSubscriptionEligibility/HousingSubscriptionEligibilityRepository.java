package com.mysite.applyhome.housingSubscriptionEligibility;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface HousingSubscriptionEligibilityRepository extends JpaRepository<HousingSubscriptionEligibility, Integer> {
    @Query("SELECT h FROM HousingSubscriptionEligibility h " +
           "WHERE h.personalProfiles.residentRegistrationNumber = :residentRegistrationNumber " +
           "ORDER BY h.lastAssessmentDate DESC LIMIT 1")
    HousingSubscriptionEligibility findTopByPersonalProfiles_ResidentRegistrationNumberOrderByLastAssessmentDateDesc(
        @Param("residentRegistrationNumber") String residentRegistrationNumber);
} 