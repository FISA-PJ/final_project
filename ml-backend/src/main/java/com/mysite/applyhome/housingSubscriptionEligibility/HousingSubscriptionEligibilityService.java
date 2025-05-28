package com.mysite.applyhome.housingSubscriptionEligibility;

import com.mysite.applyhome.user.Users;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class HousingSubscriptionEligibilityService {

    private final HousingSubscriptionEligibilityRepository eligibilityRepository;

    public HousingSubscriptionEligibilityService(HousingSubscriptionEligibilityRepository eligibilityRepository) {
        this.eligibilityRepository = eligibilityRepository;
    }

    public String getEligibilityPrimeType(Users user) {
        if (user == null || user.getPersonalProfiles() == null) {
            throw new IllegalArgumentException("사용자 정보가 올바르지 않습니다.");
        }

        HousingSubscriptionEligibility eligibility = eligibilityRepository
            .findTopByPersonalProfiles_ResidentRegistrationNumberOrderByLastAssessmentDateDesc(
                user.getPersonalProfiles().getResidentRegistrationNumber());
        
        return eligibility != null ? eligibility.getEligibilityPrimeType() : "미평가";
    }

    public String getEligibilitySubType(Users user) {
        if (user == null || user.getPersonalProfiles() == null) {
            throw new IllegalArgumentException("사용자 정보가 올바르지 않습니다.");
        }

        HousingSubscriptionEligibility eligibility = eligibilityRepository
            .findTopByPersonalProfiles_ResidentRegistrationNumberOrderByLastAssessmentDateDesc(
                user.getPersonalProfiles().getResidentRegistrationNumber());
        
        return eligibility != null ? eligibility.getEligibilitySubType() : "미평가";
    }
} 