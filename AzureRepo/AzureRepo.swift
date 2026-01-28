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

    /// Stores a local file as an item in the repository
    func put(_ identifier: String, fromFile local_file_path: String) async throws {
        let fileURL = URL(fileURLWithPath: local_file_path)
        let data = try Data(contentsOf: fileURL)
        try await put(identifier, content: data)
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
