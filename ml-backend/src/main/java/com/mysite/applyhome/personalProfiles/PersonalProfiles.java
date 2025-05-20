package com.mysite.applyhome.personalProfiles;

import jakarta.persistence.*;
import jakarta.validation.constraints.*;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Getter
@Setter
@Entity
@Table(name = "Personal_Profiles", uniqueConstraints = {
        @UniqueConstraint(columnNames = "resident_registration_number")
})
public class PersonalProfiles {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer personal_id;

    @Column(name = "personal_name", nullable = false, length = 50)
    @NotBlank(message = "개인 이름은 필수 항목입니다.")
    private String personalName;

    @Column(nullable = false)
    @NotNull(message = "생년월일은 필수 항목입니다.")
    private LocalDate personal_birth_date;

    @Column(name = "resident_registration_number", nullable = false, length = 13, unique = true)
    @NotBlank(message = "주민등록번호는 필수 항목입니다.")
    @Pattern(regexp = "\\d{12,13}", message = "주민등록번호는 숫자 12~13자리여야 합니다.")
    private String residentRegistrationNumber;

    @Column(nullable = false, columnDefinition = "DATETIME DEFAULT CURRENT_TIMESTAMP")
    private LocalDateTime created_at;

    @Column(nullable = false, columnDefinition = "DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
    private LocalDateTime updated_at;

    @Column(name = "marriage_status", nullable = false, columnDefinition = "TINYINT(1) DEFAULT 0")
    private boolean marriageStatus;
}
