package com.mysite.sbb.user;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Service
public class UserSecurityService implements UserDetailsService {

	private final UsersRepository usersRepository;

	@Override
	public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
		Optional<Users> _siteUser = this.usersRepository.findByUserLoginId(username);
		if (_siteUser.isEmpty()) {
			throw new UsernameNotFoundException("사용자를 찾을수 없습니다.");
		}
		Users users = _siteUser.get();
//		List<GrantedAuthority> authorities = new ArrayList<>();
//		if ("admin".equals(username)) {
//			authorities.add(new SimpleGrantedAuthority(UserRole.ADMIN.getValue()));
//		} else {
//			authorities.add(new SimpleGrantedAuthority(UserRole.USER.getValue()));
//		}
		return new SiteUserDetails(users);
		//return new org.springframework.security.core.userdetails.User(users.getUserLoginId(), users.getUserPasswordHash(), authorities);
	}
}
