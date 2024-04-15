import { ref, computed } from 'vue'

export default function useCatalog(initJsonData) {
  const catalogJson = ref(initJsonData)
  const linksTotal = computed(() => {
    return links.value.length
  })
  const links = computed(() => {
    if (Object.prototype.hasOwnProperty.call(catalogJson.value, 'links')) {
      return catalogJson.value.links
    } else {
      return []
    }
  })
  const childLinks = computed(() => {
    let children = []
    links.value.forEach(link => {
      if (link.rel === 'child' && link.type === 'text/html') {
        const splitLink = link.href.split('/')
        link.name = splitLink[splitLink.length-1] // last path
        children.push(link)
      }
    })
    return children
  })
  const itemLinks = computed(() => {
    let children = []
    links.value.forEach(link => {
      if (link.rel === 'item') {
        link.name = link.title
        children.push(link)
      }
    })
    return children
  })

  return {
    links, childLinks, itemLinks,
    linksTotal,
    catalogJson
  }
}