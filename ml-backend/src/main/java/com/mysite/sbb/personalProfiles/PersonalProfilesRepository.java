package com.mysite.sbb.personalProfiles;

import com.mysite.sbb.question.Question;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PersonalProfilesRepository extends JpaRepository<PersonalProfiles, Integer> {
    PersonalProfiles findByResidentRegistrationNumber(String residentId);
}