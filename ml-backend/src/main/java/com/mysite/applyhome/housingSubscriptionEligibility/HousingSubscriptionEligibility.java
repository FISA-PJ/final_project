package com.mysite.applyhome.housingSubscriptionEligibility;

import com.mysite.applyhome.personalProfiles.PersonalProfiles;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import java.time.LocalDateTime;

@Entity
@Getter
@Setter
@Table(name = "Housing_Subscription_Eligibility")
public class HousingSubscriptionEligibility {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "eligibility_id")
    private Integer eligibilityId;

    @OneToOne
    @JoinColumn(
        name = "resident_registration_number",
        referencedColumnName = "resident_registration_number",
        nullable = false,
        foreignKey = @ForeignKey(name = "FK_Personal_Profiles_TO_Housing_Subscription_Eligibility")
    )
    private PersonalProfiles personalProfiles;

    @Column(name = "eligibility_prime_type", nullable = false, length = 20)
    private String eligibilityPrimeType;

    @Column(name = "eligibility_sub_type", nullable = false, length = 20)
    private String eligibilitySubType;

    @Column(name = "last_assessment_date", nullable = false)
    private LocalDateTime lastAssessmentDate;
} 