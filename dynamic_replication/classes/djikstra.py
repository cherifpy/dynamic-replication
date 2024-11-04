import heapq

def djikstra(graph, start_node, end_node):
    num_nodes = len(graph)
    distances = [float('inf')] * num_nodes
    distances[start_node] = 0
    priority_queue = [(0, start_node)]
    previous_nodes = [None] * num_nodes

    while priority_queue:
        current_distance, current_node = heapq.heappop(priority_queue)

        if current_node == end_node:
            break

        if current_distance > distances[current_node]:
            continue

        for neighbor in range(num_nodes):
            if graph[current_node][neighbor] > 0:  # Lien direct
                distance = current_distance + graph[current_node][neighbor]
                if distance < distances[neighbor]:
                    distances[neighbor] = distance
                    previous_nodes[neighbor] = current_node
                    heapq.heappush(priority_queue, (distance, neighbor))

    # Reconstruire le chemin le plus court
    path = []
    current_node = end_node
    while current_node is not None:
        path.append(current_node)
        current_node = previous_nodes[current_node]
    path.reverse()

    return path, distances[end_node]


if __name__ == '__main__':
    graph = [
        [0, 0, 1, 1, 0, 0],   # Nœud 0
        [0, 0, 0, 0, 1, 0],   # Nœud 1
        [1, 0, 0, 1, 1, 1],   # Nœud 2
        [1, 0, 1, 0, 0, 0],   # Nœud 3
        [0, 1, 1, 0, 0, 0],   # Nœud 4
        [0, 0, 1, 0, 0, 0]    # Nœud 5
    ]
    # Exemples d'utilisation
    start_node = 0
    end_node = 4
    path, cost = djikstra(graph, start_node, end_node)
    print(f"Chemin le plus court: {path} avec un coût de {cost}")
