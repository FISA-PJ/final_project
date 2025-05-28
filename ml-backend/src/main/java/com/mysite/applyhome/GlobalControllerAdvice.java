package com.mysite.applyhome;

import com.mysite.applyhome.user.SiteUserDetails;
import com.mysite.applyhome.housingSubscriptionEligibility.HousingSubscriptionEligibilityService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ModelAttribute;

@RequiredArgsConstructor
@ControllerAdvice
public class GlobalControllerAdvice {

    private final HousingSubscriptionEligibilityService eligibilityService;

    @ModelAttribute("personalName")
    public String populatePersonalName(Authentication authentication) {
        if (authentication != null && authentication.getPrincipal() instanceof SiteUserDetails) {
            SiteUserDetails userDetails = (SiteUserDetails) authentication.getPrincipal();
            return userDetails.getUser().getPersonalProfiles().getPersonalName();
        }
        return null;
    }

    @ModelAttribute("primeType")
    public String populatePrimeType(Authentication authentication) {
        if (authentication != null && authentication.getPrincipal() instanceof SiteUserDetails) {
//            String primeType = eligibilityService.getEligibilityPrimeType(userService.getUser(principal.getName()));
//            return ResponseEntity.ok(primeType);
            SiteUserDetails userDetails = (SiteUserDetails) authentication.getPrincipal();
            return eligibilityService.getEligibilityPrimeType(userDetails.getUser());
                   // .getPersonalProfiles().getPersonalName();
        }
        return null;
    }
}
