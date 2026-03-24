//
//  Untitled.swift
//  AzureRepo
//
//  Created by Küng, Steve on 9/1/25.
//

import Foundation

// Error class for Azure-specific errors
class AzureRepoError: Error, CustomStringConvertible {
    private let message: String
    public init(_ message: String) {
        self.message = message
    }
    public var description: String { message }
}

extension AzureRepoError: LocalizedError {
    var errorDescription: String? { message }
}


// Main class for the Azure Munki repository
class AzureRepo: Repo {
    var baseURL: URL
    var sasToken: String = ""

    required init(_ url: String) throws {
        guard let baseURL = URL(string: url) else {
            throw AzureRepoError("Could not create valid URL from \(url)")
        }
        self.baseURL = baseURL
        try getSASToken()
    }

    /// Retrieves the SAS token from environment variables or prompts interactively
    private func getSASToken() throws {
        if sasToken.isEmpty {
            let env = ProcessInfo.processInfo.environment
            if let tokenFromEnv = env["SAS_TOKEN"] {
                sasToken = tokenFromEnv
                return
            }
            print("Connecting to Azure Blob Storage at \(baseURL)...")
            print("SAS Token: ", terminator: "")
            if let input = readLine(strippingNewline: true) {
                sasToken = input
            }
            if sasToken.isEmpty {
                throw AzureRepoError("No SAS Token provided.")
            }
        }
    }

    /// Builds the Azure Storage URL with SAS token
    private func buildAzureStorageURL(_ originalURL: String, sas: String) -> String {
        if sas.hasPrefix("?") {
            return originalURL + sas
        }
        return originalURL + "?" + sas
    }

    /// Returns a list of items for the given kind (e.g. "catalogs", "manifests", "pkgsinfo", "pkgs", "icons")
    func list(_ kind: String) async throws -> [String] {
        // List blobs in the container with prefix 'kind'
        let containerName = baseURL.lastPathComponent
        let storageBase = baseURL.deletingLastPathComponent().absoluteString
        let endpoint = storageBase + containerName + "/?restype=container&comp=list"
        let urlString = buildAzureStorageURL(endpoint, sas: sasToken)
        guard let url = URL(string: urlString) else {
            throw AzureRepoError("Invalid Azure Storage URL")
        }
        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        let (data, response) = try await URLSession.shared.data(for: request)
        guard let httpResponse = response as? HTTPURLResponse, (200...299).contains(httpResponse.statusCode) else {
            throw AzureRepoError("Azure Storage list failed")
        }
        // Parse XML response to get blob names
        let xml = String(data: data, encoding: .utf8) ?? ""
        var result: [String] = []
        let pattern = "<Name>(.*?)</Name>"
        let regex = try? NSRegularExpression(pattern: pattern, options: [])
        let matches = regex?.matches(in: xml, options: [], range: NSRange(location: 0, length: xml.utf16.count)) ?? []
        for match in matches {
            if let range = Range(match.range(at: 1), in: xml) {
                let name = String(xml[range])
                if name.hasPrefix(kind) {
                    result.append(String(name.dropFirst(kind.count + (kind.hasSuffix("/") ? 0 : 1))))
                }
            }
        }
        return result
    }

    /// Retrieves an item as Data
    func get(_ identifier: String) async throws -> Data {
        let containerName = baseURL.lastPathComponent
        let storageBase = baseURL.deletingLastPathComponent().absoluteString
        let endpoint = storageBase + containerName + "/" + identifier
        let urlString = buildAzureStorageURL(endpoint, sas: sasToken)
        guard let url = URL(string: urlString) else {
            throw AzureRepoError("Invalid Azure Storage URL")
        }
        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        let (data, response) = try await URLSession.shared.data(for: request)
        guard let httpResponse = response as? HTTPURLResponse, (200...299).contains(httpResponse.statusCode) else {
            throw AzureRepoError("Azure Storage get failed for \(identifier)")
        }
        return data
    }

    /// Retrieves an item and saves it to a file
    func get(_ identifier: String, toFile local_file_path: String) async throws {
        let data = try await get(identifier)
        FileManager.default.createFile(atPath: local_file_path, contents: data)
    }

    /// Stores Data as an item in the repository
    func put(_ identifier: String, content: Data) async throws {
        let containerName = baseURL.lastPathComponent
        let storageBase = baseURL.deletingLastPathComponent().absoluteString
        let endpoint = storageBase + containerName + "/" + identifier
        let urlString = buildAzureStorageURL(endpoint, sas: sasToken)
        guard let url = URL(string: urlString) else {
            throw AzureRepoError("Invalid Azure Storage URL")
        }
        var request = URLRequest(url: url)
        request.httpMethod = "PUT"
        request.httpBody = content
        let (_, response) = try await URLSession.shared.data(for: request)
        guard let httpResponse = response as? HTTPURLResponse, (200...299).contains(httpResponse.statusCode) else {
            throw AzureRepoError("Azure Storage put failed for \(identifier)")
        }
    }

    /// Stores a local file as an item in the repository.
    /// Files larger than 4 GB are uploaded using Azure Block Blob API in 100 MB chunks.
    func put(_ identifier: String, fromFile local_file_path: String) async throws {
        let attributes = try FileManager.default.attributesOfItem(atPath: local_file_path)
        let fileSize = attributes[.size] as? Int ?? 0

        // Single PUT works up to ~5 GB; use block upload for anything above 4 GB
        if fileSize <= 4 * 1024 * 1024 * 1024 {
            let fileURL = URL(fileURLWithPath: local_file_path)
            let data = try Data(contentsOf: fileURL)
            try await put(identifier, content: data)
            return
        }

        try await putLargeFile(identifier, filePath: local_file_path, fileSize: fileSize)
    }

    /// Block size for chunked uploads (100 MB)
    private static let blockSize = 100 * 1024 * 1024

    /// Uploads a large file using Azure Block Blob API (Put Block + Put Block List).
    /// Streams the file in 100 MB chunks to avoid loading it entirely into memory.
    private func putLargeFile(_ identifier: String, filePath: String, fileSize: Int) async throws {
        let containerName = baseURL.lastPathComponent
        let storageBase = baseURL.deletingLastPathComponent().absoluteString
        let blobEndpoint = storageBase + containerName + "/" + identifier

        guard let fileHandle = FileHandle(forReadingAtPath: filePath) else {
            throw AzureRepoError("Cannot open file \(filePath)")
        }
        defer { fileHandle.closeFile() }

        let blockSize = AzureRepo.blockSize
        let blockCount = (fileSize + blockSize - 1) / blockSize
        var blockIDs: [String] = []

        for i in 0..<blockCount {
            let blockID = String(format: "%06d", i)
            let blockIDEncoded = Data(blockID.utf8).base64EncodedString()
            blockIDs.append(blockIDEncoded)

            let blockData = fileHandle.readData(ofLength: blockSize)

            let blockURL = blobEndpoint + "&comp=block&blockid=" + blockIDEncoded
            let urlString = buildAzureStorageURL(blockURL, sas: sasToken)
            guard let url = URL(string: urlString) else {
                throw AzureRepoError("Invalid Azure Storage URL for block \(i)")
            }
            var request = URLRequest(url: url)
            request.httpMethod = "PUT"
            request.setValue("\(blockData.count)", forHTTPHeaderField: "Content-Length")
            request.httpBody = blockData

            let (_, response) = try await URLSession.shared.data(for: request)
            guard let httpResponse = response as? HTTPURLResponse, (200...299).contains(httpResponse.statusCode) else {
                let code = (response as? HTTPURLResponse)?.statusCode ?? -1
                throw AzureRepoError("Azure Storage put block \(i + 1)/\(blockCount) failed (HTTP \(code)) for \(identifier)")
            }
            print("Uploaded block \(i + 1)/\(blockCount) for \(identifier)")
        }

        // Commit all blocks with Put Block List
        let blockListXML = "<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList>"
            + blockIDs.map { "<Latest>\($0)</Latest>" }.joined()
            + "</BlockList>"

        let commitURL = blobEndpoint + "&comp=blocklist"
        let commitURLString = buildAzureStorageURL(commitURL, sas: sasToken)
        guard let url = URL(string: commitURLString) else {
            throw AzureRepoError("Invalid Azure Storage URL for block list commit")
        }
        var request = URLRequest(url: url)
        request.httpMethod = "PUT"
        request.setValue("application/xml", forHTTPHeaderField: "Content-Type")
        request.httpBody = Data(blockListXML.utf8)

        let (_, response) = try await URLSession.shared.data(for: request)
        guard let httpResponse = response as? HTTPURLResponse, (200...299).contains(httpResponse.statusCode) else {
            let code = (response as? HTTPURLResponse)?.statusCode ?? -1
            throw AzureRepoError("Azure Storage put block list failed (HTTP \(code)) for \(identifier)")
        }
        print("Successfully uploaded \(identifier) (\(blockCount) blocks)")
    }

    /// Deletes an item in the repository
    func delete(_ identifier: String) async throws {
        let containerName = baseURL.lastPathComponent
        let storageBase = baseURL.deletingLastPathComponent().absoluteString
        let endpoint = storageBase + containerName + "/" + identifier
        let urlString = buildAzureStorageURL(endpoint, sas: sasToken)
        guard let url = URL(string: urlString) else {
            throw AzureRepoError("Invalid Azure Storage URL")
        }
        var request = URLRequest(url: url)
        request.httpMethod = "DELETE"
        let (_, response) = try await URLSession.shared.data(for: request)
        guard let httpResponse = response as? HTTPURLResponse, (200...299).contains(httpResponse.statusCode) else {
            throw AzureRepoError("Azure Storage delete failed for \(identifier)")
        }
    }

    /// Non-filesystem-based repositories return nil
    func pathFor(_ identifier: String) -> String? {
        return nil
    }
}

// MARK: dylib "interface"

/// Function with C calling style for our dylib. We use it to instantiate the Repo object and return an instance
@_cdecl("createPlugin")
public func createPlugin() -> UnsafeMutableRawPointer {
    return Unmanaged.passRetained(AzureRepoBuilder()).toOpaque()
}

final class AzureRepoBuilder: RepoPluginBuilder {
    override func connect(_ url: String) -> Repo? {
        return try? AzureRepo(url)
    }
}
