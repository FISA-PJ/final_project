package com.mysite.applyhome.subscript;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Controller
@RequestMapping("/subscript")
public class SubscriptController {
    @Value("${LH_API_KEY}")
    private String lhApiKey;

    @Autowired
    private SubscriptService subscriptService;

    @GetMapping({"", "/"})
    public String showSubscripts(
            Model model, 
            @RequestParam(value = "page", defaultValue = "1") int page,
            @RequestParam(value = "type", defaultValue = "05") String type,
            @RequestParam(value = "region", required = false) String region,
            @RequestParam(value = "status", required = false) String status,
             @RequestParam(value = "dateType", required = false) String dateType,
             @RequestParam(value = "startDate", required = false) String startDate,
             @RequestParam(value = "endDate", required = false) String endDate
    ) {
        String serviceKey = lhApiKey;
        int pageSize = 10;
        
        // 서비스 호출 시 모든 필터 파라미터 전달
        Page<Subscript> paging = subscriptService.fetchSubscripts(
            serviceKey, 
            pageSize, 
            page, 
            type,
            region,
            status,
                dateType,
             startDate,
             endDate
        );
        
        // 모든 선택된 필터 값을 모델에 추가
        model.addAttribute("paging", paging);
        model.addAttribute("selectedType", type);
        model.addAttribute("selectedRegion", region);
        model.addAttribute("selectedStatus", status);
        // model.addAttribute("selectedDateType", dateType);
        // model.addAttribute("selectedStartDate", startDate);
        // model.addAttribute("selectedEndDate", endDate);
        
        return "subscript_list";
    }
}
