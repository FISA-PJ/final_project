package com.mysite.applyhome.personalProfiles;

import org.springframework.data.jpa.repository.JpaRepository;

public interface PersonalProfilesRepository extends JpaRepository<PersonalProfiles, Integer> {
    PersonalProfiles findByResidentRegistrationNumber(String residentId);
}