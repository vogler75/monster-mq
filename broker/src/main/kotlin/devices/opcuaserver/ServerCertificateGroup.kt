package at.rocworks.devices.opcuaserver

import org.eclipse.milo.opcua.stack.core.NodeIds0
import org.eclipse.milo.opcua.stack.core.NodeIds1
import org.eclipse.milo.opcua.stack.core.security.CertificateFactory
import org.eclipse.milo.opcua.stack.core.security.CertificateGroup
import org.eclipse.milo.opcua.stack.core.security.CertificateValidator
import org.eclipse.milo.opcua.stack.core.security.TrustListManager
import org.eclipse.milo.opcua.stack.core.types.builtin.ByteString
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId
import org.bouncycastle.asn1.x500.X500Name
import java.security.KeyPair
import java.security.cert.X509Certificate

/**
 * Minimal CertificateGroup implementation that exposes a single pre-loaded server
 * keypair + certificate chain (loaded from a PKCS#12 keystore) to Milo 1.x.
 *
 * Milo 1.x replaced the simple `DefaultCertificateManager(keyPair, certificate)` constructor
 * with a CertificateGroup-based model. We don't need any of the dynamic certificate-rotation
 * machinery (CertificateFactory, updateCertificate) — the keystore loader is the source of
 * truth, so those operations throw UnsupportedOperationException.
 */
class ServerCertificateGroup(
    private val keyPair: KeyPair,
    private val certificateChain: Array<X509Certificate>,
    private val trustListManager: TrustListManager,
    private val certificateValidator: CertificateValidator
) : CertificateGroup {

    private val certTypeId: NodeId = NodeIds0.RsaSha256ApplicationCertificateType

    override fun getCertificateGroupId(): NodeId =
        NodeIds1.ServerConfiguration_CertificateGroups_DefaultApplicationGroup

    override fun getSupportedCertificateTypeIds(): List<NodeId> = listOf(certTypeId)

    override fun getTrustListManager(): TrustListManager = trustListManager

    override fun getCertificateEntries(): List<CertificateGroup.Entry> =
        listOf(CertificateGroup.Entry(getCertificateGroupId(), certTypeId, certificateChain))

    override fun getKeyPair(certificateTypeId: NodeId): java.util.Optional<KeyPair> =
        if (certificateTypeId == certTypeId) java.util.Optional.of(keyPair) else java.util.Optional.empty()

    override fun getCertificateChain(certificateTypeId: NodeId): java.util.Optional<Array<X509Certificate>> =
        if (certificateTypeId == certTypeId) java.util.Optional.of(certificateChain) else java.util.Optional.empty()

    override fun updateCertificate(
        certificateTypeId: NodeId,
        keyPair: KeyPair,
        certificateChain: Array<X509Certificate>
    ) {
        throw UnsupportedOperationException("Server certificate is loaded from PKCS#12 keystore and cannot be updated at runtime")
    }

    override fun getCertificateFactory(): CertificateFactory = NoopCertificateFactory

    override fun getCertificateValidator(): CertificateValidator = certificateValidator

    private object NoopCertificateFactory : CertificateFactory {
        override fun createKeyPair(certificateTypeId: NodeId): KeyPair =
            throw UnsupportedOperationException("Certificate generation not supported; use the keystore loader")

        override fun createCertificateChain(certificateTypeId: NodeId, keyPair: KeyPair): Array<X509Certificate> =
            throw UnsupportedOperationException("Certificate generation not supported; use the keystore loader")

        override fun createSigningRequest(
            certificateTypeId: NodeId,
            keyPair: KeyPair,
            subjectName: X500Name,
            sanUri: String,
            sanDnsNames: List<String>,
            sanIpAddresses: List<String>
        ): ByteString = throw UnsupportedOperationException("CSR generation not supported")
    }
}
