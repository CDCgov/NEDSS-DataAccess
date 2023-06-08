const express = require('express')
const fetch = (...args) => import('node-fetch').then(({default:fetch})=> fetch(...args));


const PORT = 3001
const app = express()

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`)
})

async function fetchAccessToken() {
  try {
    const body = {
      username: "username-sample", // admin username
      password: "password-sample", // admin password
      provider: "db",
      refresh: true,
    }

    const response = await fetch(
      "http://localhost:8088/api/v1/security/login",
      {
        method: "POST",
        body: JSON.stringify(body),
        headers: {
          "Content-Type": "application/json",
        },
      }
    )

    const jsonResponse = await response.json()
    return jsonResponse?.access_token
  } catch (e) {
    console.error(error)
    console.log(`Error in fetchAccessToken`)
  }
}

async function fetchGuestToken() {
  const accessToken = await fetchAccessToken()
  try {
    const body = {
      resources: [
        {
          type: "dashboard",
          id: "id-example-7804", // given by the Superset embedding UI
        },
      ],
      rls: [],
      user: {
        username: "username-sample", // guest username
        first_name: "firstname-sample", // guest first name
        last_name: "lastname-sample", // guest last name
      },
    }
    const response = await fetch(
      "http://localhost:8088/api/v1/security/guest_token",
      {
        method: "POST",
        body: JSON.stringify(body),
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${accessToken}`,
        },
      }
    )
    const jsonResponse = await response.json()
    return jsonResponse?.token
  } catch (error) {
    console.error(error)
  }
}

app.get("/guest-token", async (req, res) => {
  const token = await fetchGuestToken()
  res.json(token)
})