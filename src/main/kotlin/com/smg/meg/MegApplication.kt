package com.smg.meg

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableScheduling
class MegApplication

fun main(args: Array<String>) {
    runApplication<MegApplication>(*args)
}