import { createRequire } from "node:module"
import path from "node:path"

const require = createRequire(import.meta.url)
const supported = process.platform === "darwin" || process.platform === "linux"
const arch = process.arch === "arm64" || process.arch === "x64" ? process.arch : undefined
const libc = process.platform === "linux" ? (process.report.getReport().header.glibcVersionRuntime ? "gnu" : "musl") : ""
const suffix = [process.platform, arch, libc].filter(Boolean).join("-")

export const binaryPath =
  supported && arch
    ? path.join(path.dirname(require.resolve(`@opencode-ai/opencode-pty-${suffix}/package.json`)), "bin", "opencode-pty")
    : undefined
