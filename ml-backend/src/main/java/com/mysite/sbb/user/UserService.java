package com.mysite.sbb.user;

import java.util.Optional;

import com.mysite.sbb.personalProfiles.PersonalProfiles;
import com.mysite.sbb.personalProfiles.PersonalProfilesRepository;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import com.mysite.sbb.DataNotFoundException;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Service
public class UserService {

	private final UsersRepository usersRepository;
	private final PasswordEncoder passwordEncoder;
	private final PersonalProfilesRepository personalProfilesRepository;

	public Users create(String username, String residentId, String password) {
		Users users = new Users();
		users.setUserLoginId(username);
//		users.setUserRegistrationNumber(residentId);

//		PersonalProfiles profile = personalProfilesRepository.findByResidentRegistrationNumber(residentId);
//		users.setPersonalProfiles(profile);

		users.setUserPasswordHash(passwordEncoder.encode(password));
		this.usersRepository.save(users);
		return users;
	}

	public Users getUser(String username) {
		Optional<Users> siteUser = this.usersRepository.findByUserLoginId(username);
		if (siteUser.isPresent()) {
			return siteUser.get();
		} else {
			throw new DataNotFoundException("siteuser not found");
		}
	}
}
