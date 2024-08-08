package at.rocworks

import java.io.Serializable

data class ClientId(val identifier: String): Serializable {
    override fun toString() = identifier
}