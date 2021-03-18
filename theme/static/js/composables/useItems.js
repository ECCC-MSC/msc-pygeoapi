import { ref, computed, onMounted, watch } from 'https://cdnjs.cloudflare.com/ajax/libs/vue/3.0.7/vue.esm-browser.prod.js'

export default function useItems() {
  const requestUrl = ref('')
  const limit = ref(10) // default
  const currentPage = ref(1)
  const itemsLoading = ref(false)
  const itemsJson = ref({})
  const itemsTotal = computed(() => {
    if (Object.prototype.hasOwnProperty.call(itemsJson.value, 'numberMatched')) {
      return itemsJson.value.numberMatched
    } else {
      return 500
    }
  })
  const items = computed(() => {
    if (Object.prototype.hasOwnProperty.call(itemsJson.value, 'features')) {
      // map items to only show its properties
      return itemsJson.value.features.map((item) => {
        return {
          id: item.id, // include default "id"
          ...item.properties
        }
      })
    } else {
      return []
    }
  })
  const itemProps = computed(() => {
    if (items.value.length > 0) {
      return Object.keys(items.value[0])
    } else {
      return []
    }
  })
  const showingLimitText = computed(() => {
    let showText = `Showing ${parseInt(startindex.value) + 1} to ${parseInt(startindex.value) + parseInt(limit.value)} of ${itemsTotal.value}`
    return showText
  })
  const startindex = computed(() => {
    if (currentPage.value === 1) {
      return 0
    } else {
      return parseInt((currentPage.value - 1) * limit.value)
    }
  })
  const maxPages = computed(() => {
    return parseInt(itemsTotal.value / limit.value)
  })
  const nextPage = function() {
    if ((currentPage.value * limit.value) < itemsTotal.value) {
      currentPage.value++
      getItems()
    }
  }
  const prevPage = function() {
    if (currentPage.value > 1) {
      currentPage.value--
      getItems()
    }
  }
  const getItems = async () => {
    try {
      itemsLoading.value = true
      const newRequestUrl = `?f=json&limit=${limit.value}&startindex=${startindex.value}`  // relative to /items
      if (requestUrl.value === newRequestUrl) {
        return false // prevent duplicate calls
      }
      requestUrl.value = newRequestUrl
      const resp = await axios.get(requestUrl.value)
      itemsJson.value = resp.data
      itemsLoading.value = false
    } catch (err) {
      console.error(err)
      itemsLoading.value = false
    }
  }
  
  onMounted(getItems)
  watch(limit, () => {
    currentPage.value = 1 // reset for limit
  })

  return {
    itemsJson, itemsTotal, items, itemProps, limit,
    getItems, showingLimitText, itemsLoading,
    nextPage, prevPage, currentPage, maxPages
  }
}