package com.example.springrabbittest

import java.io.Serializable

data class SuperData(val id: Int, val value: String) : Serializable {
    companion object {
        const val serialVersionUID = 1L
    }
}