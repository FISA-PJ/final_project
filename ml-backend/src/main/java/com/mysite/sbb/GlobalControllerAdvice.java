package com.mysite.sbb;

import com.mysite.sbb.user.SiteUserDetails;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ModelAttribute;

@ControllerAdvice
public class GlobalControllerAdvice {

    @ModelAttribute("personalName")
    public String populatePersonalName(Authentication authentication) {
        if (authentication != null && authentication.getPrincipal() instanceof SiteUserDetails) {
            SiteUserDetails userDetails = (SiteUserDetails) authentication.getPrincipal();
            return userDetails.getUser().getPersonalProfiles().getPersonalName();
        }
        return null;
    }
}
