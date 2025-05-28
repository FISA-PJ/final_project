package com.mysite.applyhome.housingSubscriptionEligibility;

import com.mysite.applyhome.user.UserService;
import com.mysite.applyhome.user.Users;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.security.Principal;

@RequiredArgsConstructor
@RestController
@RequestMapping("/api/eligibility")
public class HousingSubscriptionEligibilityController {

    private final HousingSubscriptionEligibilityService eligibilityService;
    private final UserService userService;

//    public HousingSubscriptionEligibilityController(HousingSubscriptionEligibilityService eligibilityService) {
//        this.eligibilityService = eligibilityService;
//    }

    @PreAuthorize("isAuthenticated()")
    @GetMapping("/prime-type")
    public ResponseEntity<?> getEligibilityPrimeType(Principal principal) {
        System.out.println(principal.getName());
        if (principal == null) {
            System.out.println("user null");
            return ResponseEntity.status(401).body("로그인이 필요합니다.");
        }
//        System.out.println("user not null");
//        System.out.println(user);
        try {
            String primeType = eligibilityService.getEligibilityPrimeType(userService.getUser(principal.getName()));
            return ResponseEntity.ok(primeType);
        } catch (Exception e) {
            System.out.println(e);
            System.out.println("exception");
            return ResponseEntity.status(500).body("청약 자격 정보를 가져오는데 실패했습니다.");
        }
    }
} 