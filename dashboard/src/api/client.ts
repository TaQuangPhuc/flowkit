const BASE = ''  // same origin, proxied by Vite in dev

export async function fetchAPI<T>(path: string, options?: RequestInit, maxRetries = 3): Promise<T> {
  let attempt = 0
  while (true) {
    const res = await fetch(`${BASE}${path}`, {
      headers: { 'Content-Type': 'application/json', ...options?.headers },
      ...options,
    })

    if (res.status === 429 && attempt < maxRetries) {
      attempt++
      let waitMs = 3000
      try {
        const retryAfterHdr = res.headers.get('Retry-After')
        if (retryAfterHdr) {
          waitMs = Math.max(1000, parseFloat(retryAfterHdr) * 1000)
        } else {
          const body = await res.clone().json().catch(() => null)
          if (body?.retry_after_s) {
            waitMs = Math.max(1000, parseFloat(body.retry_after_s) * 1000)
          }
        }
      } catch {
        // fallback to default wait
      }
      console.warn(`[FlowKit API] 429 Rotation in progress on ${path}. Retrying in ${waitMs}ms (attempt ${attempt}/${maxRetries})...`)
      await new Promise(resolve => setTimeout(resolve, waitMs))
      continue
    }

    if (!res.ok) {
      const err = await res.text().catch(() => res.statusText)
      throw new Error(`API ${res.status}: ${err}`)
    }
    return res.json()
  }
}

export async function patchAPI<T>(path: string, body: Record<string, unknown>): Promise<T> {
  return fetchAPI<T>(path, { method: 'PATCH', body: JSON.stringify(body) })
}
